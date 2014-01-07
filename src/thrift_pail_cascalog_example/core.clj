(ns thrift-pail-cascalog-example.core
  (:require
   [clj-thrift.base :as thrift]
   [clj-thrift.union :as tu]
   [clj-thrift.type :as type]
   [clj-pail.core :as pl]
   [pail-cascalog.core :as pcas]
   )
  (:use cascalog.api)
  (:import [people DataUnit]
           [thrift-pail-cascalog-example DataUnitPailStructure]))


; Create some thrift objects.

(def du1-1 (thrift/build DataUnit {:property {:id "123"
                                              :property {:first_name "Eric"}}}))

(def du1-2 (thrift/build DataUnit {:property {:id "123"
                                              :property {:last_name "Gebhart"}}}))

(def  du1-3 (thrift/build DataUnit {:property {:id "123"
                                               :property { :location {:address "1 Pack Place"
                                                                      :city "Asheville"
                                                                      :state "NC"}}}}))

(def du2-1 (thrift/build DataUnit {:property {:id "abc"
                                              :property {:first_name "Frederick"}}}))

(def du2-2 (thrift/build DataUnit {:property {:id "abc"
                                              :property {:last_name "Gebhart"}}}))

(def  du2-3 (thrift/build DataUnit {:property {:id "abc"
                                               :property { :location {:address "1 Wall Street"
                                                                      :city "Asheville"
                                                                      :state "NC"}}}}))

(def du3 (thrift/build DataUnit {:friendshipedge {:id1 "123" :id2 "abc"}}))

(def objectlist [du1-1 du1-2 du1-3 du2-1 du2-2 du2-3 du3])

;; thrift helpers.

; simple data extraction
(defn property-union-value
  "get a value from a union, inside a struct inside a union.
   name is the property name inside the struct.
   Union -> Struct :<name> -> Union - value."
  [du name]
  (tu/current-value (thrift/value (tu/current-value du) name)))

(defn property-value [du name]
  "get the named field value from the current structure in the top level union.
   Union -> struct :<name> - value."
  (thrift/value (tu/current-value du) name))

(defn get-property-id [du]
  "get the id field from the current structure in the top level union."
  (property-value du :id))


;; pail helpers
(defn find-or-create [pstruct path & {:as create-key-args}]
  "Get a pail from a path, or create one if not found"
  (try (pl/pail path)
       (catch Exception e
          (apply pl/create (pl/spec pstruct) path (mapcat identity create-key-args)))))

(defn write-objects
  "Write a list of objects to a pail"
  [pail objects]
  (with-open [writer (.openWrite pail)]
    (doseq [o objects]
        (.writeObject writer o))))



(defn write-them [pail-connection]
(with-open [writer (.openWrite pail-connection)]
    (doto writer
        (.writeObject du1-1)
        (.writeObject du1-2)
        (.writeObject du1-3)
        (.writeObject du2-1)
        (.writeObject du2-2)
        (.writeObject du2-3)
        (.writeObject du3))))


; Create Cascalog Taps. - functions to make it easy.
(defn first-name-tap [pail-connection]
  (pcas/pail->tap pail-connection :attributes [["property" "first_name"]] ))

(defn last-name-tap [pail-connection]
  (pcas/pail->tap pail-connection :attributes [["property" "last_name"]] ))

(defn location-tap [pail-connection]
  (pcas/pail->tap pail-connection :attributes [["property" "location"]] ))

(defn friendedge-tap [pail-connection]
  (pcas/pail->tap pail-connection :attributes [["friendshipedge"]] ))


;Cascalog helpers

; simple property deconstruction; age, first_name, last_name, etc.

(defmapfn sprop [du]
    "Deconstruct a simple property object that only has an id and one value which
     is a union named 'property'."
     [(property-value du :id) (property-union-value du :property) ])

;location property deconstruction.
(defmapfn locprop
  "Deconstruct a location property object, which has an id and a location struct"
  [du]
  (into [(property-value du :id)]
        (map #(thrift/value (property-union-value du :property) %)
             [:address :city :county :state :country :zip])))

(def mypail (find-or-create ( DataUnitPailStructure.) "example_output"))

; This works too. But I think find-or-create is better.
#_(def mypail (-> (DataUnitPailStructure.)
                         (pl/spec)
                         (pl/create "example_output"  :fail-on-exists false)))


(defn get-names [pail-connection]
    (let [fntap (first-name-tap pail-connection)]
      (??<- [?id ?first-name]
            (fntap ?fn-data)
            (sprop ?fn-data :> ?id ?first-name))))


(defn get-full-names [pail-connection]
  (let [fntap (first-name-tap pail-connection)
        lntap (last-name-tap pail-connection)]
    (??<- [?first-name ?last-name]
          (fntap _ ?fn-data)
          (lntap _ ?ln-data)
          (sprop ?fn-data :> ?id ?first-name)
          (sprop ?ln-data :> ?id ?last-name))))

(defn get-everything [pail-connection]
  (let [fntap (first-name-tap pail-connection)
        lntap (last-name-tap pail-connection)
        loctap (location-tap pail-connection)]
    (??<- [?first-name ?last-name !address !city !county !state !country !zip]
          (fntap _ ?fn-data)
          (lntap _ ?ln-data)
          (loctap _ ?loc-data)
          (sprop ?fn-data :> ?id ?first-name)
          (sprop ?ln-data :> ?id ?last-name)
          (locprop ?loc-data :> ?id !address !city !county !state !country !zip))))


(defn tests []
  (let [pail-struct (thrift-pail-cascalog-example.DataUnitPailStructure.)]
    ; see which partitioner we have
    (println (.getPartitioner pail-struct))
    ; print target partitions
    (prn-str [
              (.getTarget pail-struct du1-1)
              (.getTarget pail-struct du2-1)
              (.getTarget pail-struct du3)
              ])
   )
  (let [pc (find-or-create (DataUnitPailStructure.) "example_output")
        fntap (first-name-tap pc)
        loctap (location-tap pc)]
    ;print objects and their deconstructed values
    (println du1-1)
    (println (sprop du1-1))
    (println du1-2)
    (println (sprop du1-2))
    (println du1-3)
    (println (locprop du1-3))
    (println (locprop du1-3))

    ; write the objects to the pail
    (write-objects pc objectlist)

    ; Query the data back out.
    (def names (??<- [?id ?first-name]
                     (fntap _ ?fn-data)
                     (sprop ?fn-data :> ?id ?first-name)))

    (def locs (??<- [?id !address !city !county !state !country !zip]
                    (loctap _ ?loc-data)
                    (locprop ?loc-data :> ?id !address !city !county !state !country !zip)))

    (println "Names===========================")
    (println names)
    (println "Locations===========================")
    (println locs)

))


(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  ;; work around dangerous default behaviour in Clojure
  (alter-var-root #'*read-eval* (constantly false))
  (println "Hello, World!")
  (tests))
