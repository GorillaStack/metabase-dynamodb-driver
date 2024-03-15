(ns metabase.driver.dynamodb.query-processor
  (:require [clojure.string :as str]
            [metabase.mbql.schema :as mbql.s]
            [metabase.mbql.util :as mbql.u]
            [metabase.models.field :refer [Field]]
            [metabase.query-processor.store :as qp.store]
            [metabase.driver.dynamodb.util :refer [*dynamodb-client*]]))

(def ^:dynamic ^:private *query* nil)

(defn list-tables []
  (-> (.listTables *dynamodb-client*)
      (.getTableNames)))

(defn- dynamodb-type->base-type [attr-type]
  (case attr-type
    "N"      :type/Decimal
    "S"      :type/Text
    "BOOL"   :type/Boolean
    :type/*))

(defn describe-table [table]
  (let [table-desc (-> (.describeTable *dynamodb-client* table)
                       (.getTable))]
    (println "describe-table" table-desc)
    (for [attribute-def (.getAttributeDefinitions table-desc)]
      {:name      (.getAttributeName attribute-def)
       :database-type (.getAttributeType attribute-def)
       :base-type (dynamodb-type->base-type (.getAttributeType attribute-def))
       :database-position (.getId attribute-def)})) )

(defmulti ^:private ->rvalue
  "Format this `Field` or value for use as the right hand value of an expression, e.g. by adding `$` to a `Field`'s
  name"
  {:arglists '([x])}
  mbql.u/dispatch-by-clause-name-or-class)

(defmulti ^:private ->lvalue
  "Return an escaped name that can be used as the name of a given Field."
  {:arglists '([field])}
  mbql.u/dispatch-by-clause-name-or-class)

(defmulti ^:private ->initial-rvalue
  "Return the rvalue that should be used in the *initial* projection for this `Field`."
  {:arglists '([field])}
  mbql.u/dispatch-by-clause-name-or-class)

(defmethod ->rvalue :default
  [x]
  x)

(defmethod ->lvalue :field
  [[_ id-or-name]]
  (->lvalue (qp.store/field id-or-name)))

(defmethod ->rvalue :field
  [[_ id-or-name]]
  (->rvalue (qp.store/field id-or-name)))

;; (defn- with-lvalue-temporal-bucketing [field unit]
;;   (if (= unit :default)
;;     field
;;     (str field "___" (name unit))))

;; (defmethod ->lvalue :field
;;   [[_ id-or-name {:keys [temporal-unit]}]]
;;   (cond-> (if (integer? id-or-name)
;;             (->lvalue (qp.store/field id-or-name))
;;             (name id-or-name))
;;     temporal-unit (with-lvalue-temporal-bucketing temporal-unit)))

;; (defmethod ->rvalue :field
;;   [[_ id-or-name {:keys [temporal-unit]}]]
;;   (cond-> (if (integer? id-or-name)
;;             (->rvalue (qp.store/field id-or-name))
;;             (str \$ (name id-or-name)))
;;     temporal-unit (with-lvalue-temporal-bucketing temporal-unit)))

(defn- handle-fields [{:keys [fields]} pipeline-ctx]
  (println "handle-fields" fields)
  (if-not (seq fields)
    pipeline-ctx
    (let [new-projections (for [field fields]
                            [(->lvalue field) (->rvalue field)])]
      (-> pipeline-ctx
          (assoc :projections (map (comp keyword first) new-projections))
          (update :query conj (into {} new-projections))))))

(defn mbql->native [{{source-table-id :source-table} :query, :as query}]
  (let [{source-table-name :name} (qp.store/table source-table-id)]
    (binding [*query* query]
      (println "mbql->native:" query)
      {:projections nil
       :query       (reduce (fn [pipeline-ctx f]
                              (f (:query query) pipeline-ctx))
                            {:projections [], :query []}
                            [handle-fields])
       :collection  nil
       :mbql?       true})))

(defn execute-reducible-query
  [{{:keys [collection query mbql? projections]} :native}]
  (println "execute-reducible-query:"  query)
  {:rows []})
