using System;

namespace Averitt_RNA.DBAccess
{
    public static class SQLStrings
    {

        private const string DB_DATE_STRING_FORMAT = "yyyy-MM-dd HH:mm:ss.fffffff";

        //STAGED ORDERS
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

        //STAGED SERVICE LOCATIONS
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
        
    }
}
