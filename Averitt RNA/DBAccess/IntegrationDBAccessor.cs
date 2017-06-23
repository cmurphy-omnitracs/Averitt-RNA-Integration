using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using DBAccessUtility;
using Averitt_RNA.DBAccess.Records;

namespace Averitt_RNA.DBAccess
{
    public class IntegrationDBAccessor : DBAccessor
    {

        #region Private Members

        private log4net.ILog _Logger;

        #endregion

        #region Public Methods

        public IntegrationDBAccessor(log4net.ILog logger)
            : base(ConfigurationManager.ConnectionStrings["INTEGRATION"].ConnectionString)
        {
            _Logger = logger;
        }

        public List<StagedOrderRecord> SelectStagedOrders(string regionID, string staged)
        {
            List<StagedOrderRecord> stagedOrderRecordList = null;
            try
            {
                stagedOrderRecordList =
                    GetList(
                        SQLStrings.SELECT_STAGED_ORDERS(regionID, staged),
                        new StagedOrderRecord(),
                        "Select Staged Orders (" + regionID + ")"
                    ).Cast<StagedOrderRecord>().ToList();
            }
            catch (DatabaseException ex)
            {
                _Logger.Error("IntegrationDBAccessor | " + ex.Message, ex);
            }
            return stagedOrderRecordList;
        }

        public List<StagedServiceLocationRecord> SelectStagedServiceLocations(string regionID, string staged)
        {
            List<StagedServiceLocationRecord> stagedStagedServiceLocationList = null;
            try
            {
                stagedStagedServiceLocationList =
                    GetList(
                        SQLStrings.SELECT_STAGED_SERVICE_LOCATIONS(regionID, staged ),
                        new StagedServiceLocationRecord(),
                        "Select Staged Service Location (" + regionID + ")"
                    ).Cast<StagedServiceLocationRecord>().ToList();
            }
            catch (DatabaseException ex)
            {
                _Logger.Error("IntegrationDBAccessor | " + ex.Message, ex);
            }
            return stagedStagedServiceLocationList;
        }

        #endregion

    }
}
