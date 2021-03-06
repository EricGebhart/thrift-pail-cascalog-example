(ns thrift-pail-cascalog-example.data-unit-pail-structure
  (:require [clj-pail.structure :refer [gen-structure]]
            [pail-thrift.serializer :as s]
            [pail-thrift.partitioner :as p]
            [thrift-pail-cascalog-example.partitioner :as p2])
    (:import [people DataUnit])
  (:gen-class))

(gen-structure thrift-pail-cascalog-example.DataUnitPailStructure
               :type DataUnit
               :serializer (s/thrift-serializer DataUnit)
               ;:partitioner (p/union-partitioner DataUnit)
               :partitioner (p2/union-name-property-partitioner DataUnit)
               )
