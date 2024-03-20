(ns metabase.driver.dynamodb
  (:refer-clojure :exclude [second])
  (:require [metabase.driver :as driver]
            [metabase.driver.dynamodb.query-processor :as dynamodb.qp]
            [metabase.driver.dynamodb.util :refer [with-dynamodb-client]]
            [metabase.lib.metadata :as lib.metadata]
            [metabase.query-processor.store :as qp.store]))

(driver/register! :dynamodb)

(defmethod driver/display-name :dynamodb [_]
  "DynamoDB")

(defmethod driver/can-connect? :dynamodb [_ details]
  true)

(defmethod driver/describe-database :dynamodb [_ database]
  (with-dynamodb-client [_ database]
    {:tables (set (for [tname (dynamodb.qp/list-tables)]
                    {:schema nil, :name tname}))}))

(defmethod driver/describe-table :dynamodb [_ database {table-name :name}]
  (with-dynamodb-client [_ database]
    {:schema nil
     :name   table-name
     :fields (set (dynamodb.qp/describe-table table-name))}))

(defmethod driver/mbql->native :dynamodb [_ query]
  (dynamodb.qp/mbql->native query))

(defmethod driver/execute-reducible-query :dynamodb [_ query context respond]
  (with-dynamodb-client [_ (lib.metadata/database (qp.store/metadata-provider))]
    (dynamodb.qp/execute-reducible-query query context respond)))