(ns acetyl.parser
  (:use clojure.java.io)
  (:require [clojure.string :as str]))

(defrecord AceReader [reader]
  java.io.Closeable
  (close [_]
    (.close reader)))

(defn ace-reader
  "Open a .ace file for reading"
  [ace]
    (AceReader. (reader ace)))

(defn- null-line? [^String l]
  (or (empty? l)
      (.startsWith (.trim l) "//")))

(defn- long-text-end? [l]
  (= l "***LongTextEnd***"))

(def ^:private header-re #"^(\w+) : \"([^\"]+)\"")
(def ^:private line-re #"[A-Za-z_0-9:.-]+|\".*?[^\\]\"")

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

(defn- parse-aceline [line keep-comments?]
  (loop [[t & toks] line
         out        []]
    (cond
     (nil? t)
       out
     (= t "-C")
       (if keep-comments?
         (recur toks (conj out t))
         (recur (drop-timestamp (drop 1 toks)) out))
     :default
     (recur (drop-timestamp toks) (conj out (unquote-str t))))))

(defn- parse-aceobj [[header-line & lines] keep-comments?]
  (if-let [[_ clazz id] (re-find header-re header-line)]
    {:class clazz 
     :id id 
     :lines (vec (for [l lines]
                   (parse-aceline (re-seq line-re l) keep-comments?)))}
    (throw (Exception. (str "Bad header line: " header-line)))))
  

(defn- aceobj-seq [lines keep-comments?]
  (lazy-seq
   (let [lines       (drop-while null-line? lines)
         header-line (first lines)
         [_ clazz id] (re-find header-re (or header-line ""))]
     (cond
      (empty? header-line)
        nil
      
      (= clazz "LongText")
       (cons
        {:class "LongText"
         :id id
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
     (aceobj-seq (line-seq (:reader ace)) keep-comments?)))

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
           (line-seq $)
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
