(ns metabase.driver.dynamodb
  (:refer-clojure :exclude [second])
  (:require [metabase.driver :as driver]
            [metabase.driver.dynamodb.query-processor :as dynamodb.qp]
            [metabase.driver.sql.util.unprepare :as unprepare]
            [metabase.driver.sql-jdbc.common :as sql-jdbc.common]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
            [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
            [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
            [metabase.driver.dynamodb.util :refer [with-dynamodb-client]]))

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

(defn- prepare-query [driver {query :native, :as outer-query}]
  (cond-> outer-query
    (seq (:params query))
    (merge {:native {:params nil
                     :query (unprepare/unprepare driver (cons (:query query) (:params query)))}})))

(defmethod driver/execute-reducible-query :dynamodb
  [driver query context respond]
  ((get-method driver/execute-reducible-query :sql-jdbc) driver (prepare-query driver query) context respond))
