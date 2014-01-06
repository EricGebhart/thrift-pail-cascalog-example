(ns thrift-pail-cascalog-example.partitioner
  "Defines a Pail partitioner for Thrift unions."
  (:require [clj-pail.partitioner :as p]
            [clj-thrift.union :as union]
            [clj-thrift.base :as thrift]
            [clj-thrift.type :as type]))


(defrecord ^{:doc "A pail partitioner for Thrift unions. It requires a type, which must be a subtype
                  of `TUnion`. The partitioner will partition based on the union's set field so that
                  all union values with the same field will be placed in the same partition."}
  UnionPartitioner
  [type]

  p/VerticalPartitioner
  (p/make-partition
    [this object]
    (vector (union/current-field-id object)))

  (p/validate
    [this dirs]
    [(try
       (contains? (type/field-ids type)
                  (Integer/valueOf (first dirs)))
       (catch NumberFormatException e
         false))
     (rest dirs)]))

(defn union-partitioner
  "Returns a `UnionPartitioner`. `type` should be a subclass of `TUnion`. Note that it should be the
  class, and not an instance."
  [type]
  (UnionPartitioner. type))



(defrecord ^{:doc "A pail partitioner for Thrift unions which uses field names instead of field id's.
                  It requires a type, which must be a subtype of `TUnion`. The partitioner will partition
                  based on the union's set field name so that all union values with the same field will be
                  placed in the same partition.

                  Union PropertyValue {
                     1: name;
                     2: lastname;
                  }

                  Struct PersonProperty {
                     1: string id;
                     2: PropertyValue property;
                  }

                  Union DataUnit {
                     1: PersonProperty MyProperties;
                     2: string Things;
                  }

                  Partitioning DataUnit will result in /MyProperties and /Things as the partitions."}


  UnionNamePartitioner
  [type]


  p/VerticalPartitioner
  (p/make-partition
    [this object]
    (vector (union/current-field-name object)))


  (p/validate
    [this dirs]
    [(try
       (contains? (type/field-names type)
                  (first dirs))
       (catch Exception e
         false))
     (rest dirs)])
  )

(defn union-name-partitioner
  [type]
  (UnionNamePartitioner. type))


(defrecord ^{:doc "A 2 level pail partitioner for Thrift unions. It requires a type, which must be a subtype
                  of `TUnion`. The partitioner will partition based on the union's set field name so that
                  all union values with the same field will be placed in the same partition. If a field's
                  name is property or ends in property or Property the partitioner will also partition
                  by the union found in the :property field of that structure.

                  Union PropertyValue {
                     1: name;
                     2: lastname;
                  }

                  Struct PersonProperty {
                     1: string id;
                     2: PropertyValue property;    /* <--- this name 'property' is required. */
                  }

                  Union DataUnit {
                     1: PersonProperty MyProperties;
                     2: string Things;
                  }

                  Partitioning DataUnit will result in /1/1 (name), /1/2 (lastname), and /2 (things) as the partitions. "}

  UnionPropertyPartitioner
  [type]

  p/VerticalPartitioner
    (p/make-partition
     [this object]
     (let [res (vector (union/current-field-id object))]
       (if (re-find #"^.*[Pp]roperty$" (union/current-field-name object))
         (let [subunion (thrift/value (union/current-value object) :property)]
           (conj res (union/current-field-id subunion)))
       res)))

  (p/validate
    [this dirs]
    [(try
       (contains? (type/field-ids type)
                  (Integer/valueOf (first dirs)))
       (catch NumberFormatException e
         false))
     (rest dirs)]))

(defn union-property-partitioner
  [type]
  (UnionPropertyPartitioner. type))


(defrecord ^{:doc "A 2 level pail partitioner for Thrift unions. It requires a type, which must be a subtype
                  of `TUnion`. The partitioner will partition based on the union's set field name so that
                  all union values with the same field will be placed in the same partition. If a field's
                  name is property or ends in property or Property the partitioner will also partition
                  by the union found in the :property field of that structure.

                  Union PropertyValue {
                     1: name;
                     2: lastname;
                  }

                  Struct PersonProperty {
                     1: string id;
                     2: PropertyValue property;    /* <--- this name 'property' is required. */
                  }

                  Union DataUnit {
                     1: PersonProperty MyProperties;
                     2: string Things;
                  }

                  Partitioning DataUnit will result in /MyProperties/name, /MyProperties/lastname, and /Things as the partitions."}

  UnionNamePropertyPartitioner
  [type]


  p/VerticalPartitioner
  (p/make-partition
    [this object]
    (let [res (vector (union/current-field-name object))]
      (if (re-find #"^.*[Pp]roperty$" (first res))
        (let [subunion (thrift/value (union/current-value object) :property)]
          (conj res (union/current-field-name subunion)))
        res)))


  (p/validate
    [this dirs]
    [(try
       (contains? (type/field-names type)
                  (first dirs))
       (catch Exception e
         false))
     (rest dirs)])
  )

(defn union-name-property-partitioner
  [type]
  (UnionNamePropertyPartitioner. type))
