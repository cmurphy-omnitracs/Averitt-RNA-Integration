using System;

namespace Averitt_RNA.DBAccess
{
    public static class SQLStrings
    {

        private const string DB_DATE_STRING_FORMAT = "yyyy-MM-dd HH:mm:ss.fffffff";

        public static string SELECT_STAGED_ORDERS(
            string regionID)
        {
            return string.Format(@"
                SELECT *
                FROM [TSDBA].[STAGED_ORDERS]
                WHERE [RegionID] = '{0}'",
                regionID);
        }

    }
}
