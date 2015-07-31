(ns acetyl.parser
  (:use clojure.java.io)
  (:require [clojure.string :as str]))

(defrecord AceReader [reader]
  java.io.Closeable
  (close [_]
    (.close reader)))

(defn- ace-line-seq
  "Like line-seq but collapses down any continuation lines in the ace stream"
  [^java.io.BufferedReader rdr]
  (when-let [line (loop [line (.readLine rdr)]
                    (when line
                      (if (.endsWith line "\\")
                        (recur (str (.substring line 0 (dec (count line))) (.readLine rdr)))
                        line)))]
    (cons line (lazy-seq (ace-line-seq rdr)))))

(defn ace-reader
  "Open a .ace file for reading"
  [ace]
    (AceReader. (reader ace)))

(defn- null-line? [^String l]
  (or (empty? l)
      (.startsWith (.trim l) "//")))

(defn- long-text-end? [l]
  (= l "***LongTextEnd***"))

(def ^:private header-re #"(?m)^(\w+) *: *(?:\"([^\"]+)\"|(\w+))(?: -O (.*))?")
(def delete-re #"(?m)^-D\s+(\w+)\s+(\"\"|\"(?:[^\\\"]*|\\.)*\"|\w+)$")
(def rename-re #"(?m)^-R\s+(\w+)\s+(\"\"|\"(?:[^\\\"]*|\\.)*\"|\w+)\s(\"\"|\"(?:[^\\\"]*|\\.)*\"|\w+)$")
(def ^:private line-re #"(?m)[A-Za-z_0-9:.-]+|\"\"|\"(?:[^\\\"]*|\\.)*\"")

(defn- unquote-str [^String s]
  (if (.startsWith s "\"")
    (if (.endsWith s "\"")
      (.substring s 1 (dec (count s)))
      (throw (Exception. (str "malformed string " s))))
    s))

(defn- drop-timestamp [toks]
  (if (= (first toks) "-O")
    (drop 2 toks)
    toks))

(defn- take-timestamp [toks]
  (if (= (first toks) "-O")
    (unquote-str (second toks))))

(defn- parse-aceline [line keep-comments?]
  (loop [[t & toks] line
         out        []
         ts         []]
    (cond
     (nil? t)
       (if (some identity ts)
         (with-meta out {:timestamps ts})
         out)
     (= t "-C")
       (if keep-comments?
         (recur toks (conj out t) (conj ts nil))    ;; The -C node doesn't have a timestamp
         (recur (drop-timestamp (drop 1 toks)) out ts))
     :default
     (recur (drop-timestamp toks) (conj out (unquote-str t)) (conj ts (take-timestamp toks))))))

(defn- parse-aceobj [[header-line & lines] keep-comments?]
  (if-let [[_ clazz id] (re-find delete-re header-line)]
    {:class clazz
     :id (unquote-str id)
     :delete true}
    (if-let [[_ clazz id1 id2] (re-find rename-re header-line)]
      {:class clazz
       :id (unquote-str id1)
       :rename (unquote-str id2)}
      (if-let [[_ clazz idq idb obj-stamp] (re-find header-re header-line)]
        {:class clazz 
         :id (or idq idb)
         :timestamp (if obj-stamp
                      (unquote-str obj-stamp))
         :lines (vec (for [l lines]
                       (parse-aceline (re-seq line-re l) keep-comments?)))}
        (throw (Exception. (str "Bad header line: " header-line)))))))
  

(defn- aceobj-seq [lines keep-comments?]
  (lazy-seq
   (let [lines       (drop-while null-line? lines)
         header-line (first lines)
         [_ clazz idq idb obj-stamp] (re-find header-re (or header-line ""))]
     (cond
      (empty? header-line)
        nil
      
      (= clazz "LongText")
       (cons
        {:class "LongText"
         :id (or idq idb)
         :timestamp (if obj-stamp
                      (unquote-str obj-stamp))
         :text (->> (rest lines)
                    (take-while (complement long-text-end?))
                    (str/join "\n")
                    (.trim))}
        (aceobj-seq (->> (drop-while (complement long-text-end?) lines)
                         (rest))
                    keep-comments?))

      :default
       (let [obj-lines   (take-while (complement null-line?) lines)
             rest-lines  (drop-while (complement null-line?) lines)]
         (when (not (empty? obj-lines))
           (let [obj (parse-aceobj obj-lines keep-comments?)]
             (case (:class obj)
               "DNA"
               (cons (assoc obj
                       :text  (str/join (map first (:lines obj)))
                       :lines nil)
                     (aceobj-seq rest-lines keep-comments?))
           
               "Peptide"
               (cons (assoc obj
                       :text  (str/join (map first (:lines obj)))
                       :lines nil)
                 (aceobj-seq rest-lines keep-comments?))

               ;; default
               (cons obj
                     (aceobj-seq rest-lines keep-comments?))))))))))

(defn ace-seq
  "Return a sequence of objects from a .ace file"
  ([ace]
     (ace-seq ace false))
  ([ace keep-comments?]
     (aceobj-seq (ace-line-seq (:reader ace)) keep-comments?)))

(defn- pmatch 
  "Test whether `path` is a prefix of `l`"
  [path l]
  (and (< (count path) (count l))
       (= path (take (count path) l))))

(defn select
  "Return any lines in acedb object `obj` with leading tags matching `path`" 
  [obj path]
  (for [l (:lines obj)
        :when (pmatch path l)]
    (nthrest l (count path))))

(defn unescape [s]
  "Unescape \\-escaped characters in a string."
  (when s
    (str/replace s #"\\(.)" "$1")))

(defn- fetch-ace-block [ace pos max]
  (when (< pos max)
    (lazy-cat 
     (as-> (.transactToStream ace (str "show -a -T -b " pos " -c 100")) $
           (reader $)
           (ace-line-seq $)
           (aceobj-seq $ false)
           (doall $))
     (fetch-ace-block ace (+ pos 100) max))))

(defn query-ace [server port ace-class]
  "Minimal interface to a socket ace server"
  (let [ace (acetyl.AceSocket. server port)
        msg (.transact ace (str "find " ace-class))]
    (if-let [[_ cnt] (re-find #"// (\d+) Active Objects" msg)]
      (fetch-ace-block ace 0 (Integer/parseInt cnt)))))

(defn query-aceobj [server port ace-class name]
  "Minimal interface to a socket ace server"
  (let [ace (acetyl.AceSocket. server port)
        msg (.transact ace (str "find " ace-class " " name))]
    (first (fetch-ace-block ace 0 1))))


