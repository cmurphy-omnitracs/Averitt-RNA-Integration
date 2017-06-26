using System;

namespace Averitt_RNA.DBAccess
{
    public static class SQLStrings
    {

        private const string DB_DATE_STRING_FORMAT = "yyyy-MM-dd HH:mm:ss.fffffff";

        //GET STAGED ORDERS
        public static string SELECT_STAGED_ORDERS(
            string regionID, string Staged)
        {
            return string.Format(@"
                SELECT *
                FROM [TSDBA].[STAGED_ORDERS]
                WHERE [RegionIdentifier] = '{0}'
                And [Staged} = '{1}'",
                regionID, Staged);
        }

        //GET STAGED SERVICE LOCATIONS
        public static string SELECT_STAGED_SERVICE_LOCATIONS(
                    string regionID, string Staged)
        {
            return string.Format(@"
                SELECT *
                FROM [TSDBA].[STAGED_SERVICE_LOCATIONS] 
                WHERE [RegionIdentifier] = '{0}' 
                AND [Staged] = '{1}'",
                regionID, Staged);
        }

        //INSERT STAGED ROUTES
        public static string INSERT_STAGED_ROUTES(
                    string regionID, string orderId, string routeId, string routeStartTime, string RouteDescr, string stopSeq, string staged, string error, string status)
        {
            return string.Format(@"
                INSERT INTO STAGED_ROUTES
                (RegionIdentifier, OrderIdentifier, RouteIdentifier, RouteStartTime, RouteDescription, StopSequenceNumber, Staged, Error, Status)
                VALUES
                ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8})", regionID, orderId, routeId, routeStartTime, RouteDescr, stopSeq, staged, error, status);
        }
    }
}
