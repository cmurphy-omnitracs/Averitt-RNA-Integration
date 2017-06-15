using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Averitt_RNA.Apex;
using Averitt_RNA.DBAccess;
using WindowsServiceUtility;

namespace Averitt_RNA
{
    class MaintenanceProcessor : Processor
    {

        private long _BusinessUnitEntityKey;
        private ApexConsumer _ApexConsumer;
        private IntegrationDBAccessor _IntegrationDBAccessor;

        public MaintenanceProcessor(long businessUnitEntityKey)
            : base(MethodBase.GetCurrentMethod().DeclaringType, businessUnitEntityKey.ToString())
        {
            _BusinessUnitEntityKey = businessUnitEntityKey;
            _ApexConsumer = new ApexConsumer(businessUnitEntityKey, Logger);
            _IntegrationDBAccessor = new IntegrationDBAccessor(Logger);
        }

        public override void Process()
        {

            if (!MainService.SessionRequired)
            {

                //TODO
                //List<StagedSKURecord> stagedSKURecords = RetrieveStagedSKURecords();
                //if (stagedSKURecords != null)
                //{
                //    RetrieveExistingSKUs(stagedSKURecords);
                //    SaveSKUs(stagedSKURecords);
                //}

            }
            else
            {
                Logger.Info("Waiting for Session.");
            }

        }

    }
}
