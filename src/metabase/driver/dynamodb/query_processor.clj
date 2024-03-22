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

;; Function to list all tables in the DynamoDB client
(defn list-tables []
  (-> (.listTables *dynamodb-client*)
      (.getTableNames)))

;; Function to map DynamoDB attribute types to base types
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

;; Function to describe a table, including its attributes and their types
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

;; Function to get the components of a field name, including its parent if it has one
(defn- field-name-components [{:keys [parent-id], field-name :name, :as _field}]
  (concat
   (when parent-id
     (field-name-components (lib.metadata/field (qp.store/metadata-provider) parent-id)))
   [field-name]))

;; Function to return a single string name for a field, creating a combined qualified name for nested fields
(mu/defn field->name
  "Return a single string name for `field`. For nested fields, this creates a combined qualified name."
  ([field]
   (field->name field \.))

  ([field     :- lib.metadata/ColumnMetadata
    separator :- [:or :string char?]]
   (str/join separator (field-name-components field))))

;; Method to return the input as is for the default case
(defmethod ->rvalue :default
  [x]
  x)

;; Method to return the name of a model/Field
(defmethod ->lvalue :model/Field
  [field]
  (field->name field))

;; Method to return the lvalue of a field
(defmethod ->lvalue :field
  [[_ id-or-name]]
  (->lvalue (qp.store/field id-or-name)))

;; Method to return the rvalue of a field
(defmethod ->rvalue :field
  [[_ id-or-name]]
  (->rvalue (qp.store/field id-or-name)))

;; Function to handle fields in a pipeline context, updating projections and the query
;; This function requires updating to get GUI queries to work
(defn- handle-fields [{:keys [fields]} pipeline-ctx]
  (dynamodb.util/log "handle-fields" fields)
  (if-not (seq fields)
    pipeline-ctx
    (let [new-projections (for [field fields]
                            [(->lvalue field) (->rvalue field)])]
      (-> pipeline-ctx
          (assoc :projections (map (comp keyword first) new-projections))
          (update :query conj (into {} new-projections))))))

;; Function to convert MBQL to native query format
;; This function requires updating to get GUI queries to work
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

;; Function to execute a query on DynamoDB and return the results
(defn execute-query [parsed-query dynamodb-client]
  ;; Create a QueryRequest object and set its attributes
  (let [query-req (QueryRequest.)]
    (doto query-req
      (.setTableName (get parsed-query "TableName"))
      (.setIndexName (get parsed-query "Index"))
      (.setKeyConditionExpression (get parsed-query "KeyConditionExpression"))
      (.setExpressionAttributeValues (get parsed-query "ExpressionAttributeValues"))
      (.setExpressionAttributeNames (get parsed-query "ExpressionAttributeNames")))
    ;; Create a loop to handle paginated results
    (loop [results [] last-key nil]
      ;; Query the DynamoDB client and get the items, if last-key is not nil, use it as the ExclusiveStartKey
      (let [response (.query dynamodb-client (if last-key
                                               (.withExclusiveStartKey query-req last-key)
                                               query-req))]
        ;; Parse the items and get the last key. far/db-item->clj-item is a function to convert DynamoDB items to Clojure maps
        (let [items (-> response .getItems)
              parsed-items (mapv far/db-item->clj-item items)
              new-results (concat results parsed-items)
              next-key (.getLastEvaluatedKey response)]
          ;; If there is a next key, recur (continue loop) with the new results and the next key, otherwise return the results
          (if next-key
            (recur new-results next-key)
            new-results))))))

;; Function to execute a reducible query and respond with the results
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
      ;; Create a map of the key names from the first item
      {:cols (map (fn [key] {:name (name key)}) first-item-keys)}
      ;; Map the data from each item to a list of lists, where each list is a row of data matching the order of the columns
      (map (fn [item] (map (fn [k] (get (get item :data) k)) first-item-keys)) parsed-items))))
