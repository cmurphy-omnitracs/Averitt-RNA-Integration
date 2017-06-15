using System;
using System.Collections.Generic;
using System.Linq;
using System.Data.Odbc;
using Averitt_RNA.Apex;

namespace Averitt_RNA.DBAccess.Records
{
    public class StagedOrderRecord : DBAccessUtility.DBRecord, IEquatable<StagedOrderRecord>
    {

        #region Public Properties

        public string RegionIdentifier { get; set; }
        public string OrderIdentifier { get; set; }

        #endregion

        #region Public Methods

        override public string ToString()
        {
            return string.Format("{0} | {1}", RegionIdentifier, OrderIdentifier);
        }

        override public DBAccessUtility.DBRecord Populate(OdbcDataReader reader)
        {
            return new StagedOrderRecord
            {
                RegionIdentifier = reader["RegionIdentifier"].ToString(),
                OrderIdentifier = reader["OrderIdentifier"].ToString()
            } as DBAccessUtility.DBRecord;
        }

        public OrderSpec ToOrderSpec()
        {
            return new OrderSpec
            {
                //TODO
            };
        }

        #endregion

        #region IEquatable

        public bool Equals(StagedOrderRecord other)
        {
            //TODO
            return RegionIdentifier == other.RegionIdentifier && OrderIdentifier == other.OrderIdentifier;
        }

        public override int GetHashCode()
        {
            //TODO
            return StringComparer.InvariantCultureIgnoreCase.GetHashCode(RegionIdentifier + OrderIdentifier);
        }

        #endregion

    }
}
