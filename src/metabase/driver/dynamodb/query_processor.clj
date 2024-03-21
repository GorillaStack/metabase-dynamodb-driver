(ns metabase.driver.dynamodb.query-processor
  (:import [com.amazonaws.services.dynamodbv2.model QueryRequest])
  (:require [clojure.string :as str]
            [cheshire.core :as json]
            [medley.core :as m]
            [metabase.lib.metadata :as lib.metadata]
            [metabase.mbql.schema :as mbql.s]
            [metabase.mbql.util :as mbql.u]
            [metabase.models.field :refer [Field]]
            [metabase.query-processor.context :as qp.context]
            [metabase.query-processor.store :as qp.store]
            [metabase.query-processor.reducible :as qp.reducible]
            [metabase.driver.dynamodb.util :as dynamodb.util]
            [metabase.driver.dynamodb.util :refer [*dynamodb-client*]]
            [metabase.util.malli :as mu]
            [taoensso.faraday :as far]))

(def ^:dynamic ^:private *query* nil)

(defn list-tables []
  (-> (.listTables *dynamodb-client*)
      (.getTableNames)))

(defn- dynamodb-type->base-type [attr-type]
  (case attr-type
    "M"      :type/*
    "L"      :type/Array
    "N"      :type/Decimal
    "NS"     :type/Array
    "S"      :type/Text
    "SS"     :type/Array
    "BOOL"   :type/Boolean
    "NULL"   :type/Nil
    :type/*))

(defn describe-table [table]
  (let [table-desc (-> (.describeTable *dynamodb-client* table)
                       (.getTable))]
    (dynamodb.util/log "describe-table" table-desc)
    (for [[idx attribute-def] (m/indexed (.getAttributeDefinitions table-desc))]
      {:name      (.getAttributeName attribute-def)
       :database-type (.getAttributeType attribute-def)
       :base-type (dynamodb-type->base-type (.getAttributeType attribute-def))
       :database-position idx})) )

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

(defn- field-name-components [{:keys [parent-id], field-name :name, :as _field}]
  (concat
   (when parent-id
     (field-name-components (lib.metadata/field (qp.store/metadata-provider) parent-id)))
   [field-name]))

(mu/defn field->name
  "Return a single string name for `field`. For nested fields, this creates a combined qualified name."
  ([field]
   (field->name field \.))

  ([field     :- lib.metadata/ColumnMetadata
    separator :- [:or :string char?]]
   (str/join separator (field-name-components field))))

(defmethod ->rvalue :default
  [x]
  x)

(defmethod ->lvalue :model/Field
  [field]
  (field->name field))

(defmethod ->lvalue :field
  [[_ id-or-name]]
  (->lvalue (qp.store/field id-or-name)))

(defmethod ->rvalue :field
  [[_ id-or-name]]
  (->rvalue (qp.store/field id-or-name)))

(defn- handle-fields [{:keys [fields]} pipeline-ctx]
  (dynamodb.util/log "handle-fields" fields)
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
      (dynamodb.util/log "mbql->native:" query)
      {:projections nil
       :query       (reduce (fn [pipeline-ctx f]
                              (f (:query query) pipeline-ctx))
                            {:projections [], :query []}
                            [handle-fields])
       :collection  nil
       :mbql?       true})))

(defn execute-query [parsed-query dynamodb-client]
  (let [table-name (get parsed-query "TableName")
        index (get parsed-query "Index")
        key-condition-expression (get parsed-query "KeyConditionExpression")
        query-req (QueryRequest.)]
    (doto query-req
      (.setTableName table-name)
      (.setIndexName index)
      (.setKeyConditionExpression key-condition-expression)
      (.setExpressionAttributeValues (get parsed-query "ExpressionAttributeValues"))
      (.setExpressionAttributeNames (get parsed-query "ExpressionAttributeNames")))
    (loop [results [] last-key nil]
      (let [response (.query dynamodb-client (if last-key
                                               (.withExclusiveStartKey query-req last-key)
                                               query-req))]
        (let [items (-> response .getItems)
              parsed-items (mapv far/db-item->clj-item items)
              new-results (concat results parsed-items)
              next-key (.getLastEvaluatedKey response)]
          (if next-key
            (recur new-results next-key)
            new-results))))))

(defn execute-reducible-query
  [{{:keys [collection query mbql? projections]} :native} context respond]
  (dynamodb.util/log "execute-reducible-query:"  query)
  (dynamodb.util/log "collection:" collection)
  (let [parsed-query (json/parse-string query)
        parsed-items (execute-query parsed-query *dynamodb-client*)
        first-item (first parsed-items)
        first-item-keys (keys (get first-item :data))]
    (dynamodb.util/log "first-item:" first-item)
    (respond
      {:cols (map (fn [key] {:name (name key)}) first-item-keys)}
      (map (fn [item] (map (fn [k] (get (get item :data) k)) first-item-keys)) parsed-items))))

;; (defn execute-reducible-query
;;   [{{:keys [collection query mbql? projections]} :native} context respond]
;;   (dynamodb.util/log "execute-reducible-query:"  query)
;;   (dynamodb.util/log "collection:" collection)
;;   (let [parsed-query (json/parse-string query)
;;     table-name (get parsed-query "TableName")
;;     index (get parsed-query "Index")
;;     key-condition-expression (get parsed-query "KeyConditionExpression")
;;     query-req (QueryRequest.)]
;;     (doto query-req
;;       (.setTableName table-name)
;;       (.setIndexName index)
;;       (.setKeyConditionExpression key-condition-expression)
;;       (.setExpressionAttributeValues (get parsed-query "ExpressionAttributeValues"))
;;       (.setExpressionAttributeNames (get parsed-query "ExpressionAttributeNames")))
;;     (dynamodb.util/log "query-req:" query-req)
;;     (let [items (-> (.query *dynamodb-client* query-req) (.getItems))
;;           parsed-items (mapv far/db-item->clj-item items)
;;           first-item (first parsed-items)
;;           first-item-keys (keys (get first-item :data))]
;;       (respond
;;         {:cols (map (fn [key] {:name (name key)}) first-item-keys)}
;;         (map (fn [item] (map (fn [k] (get (get item :data) k)) first-item-keys)) parsed-items)))))
