using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Averitt_RNA.Apex;
using Averitt_RNA.DBAccess;
using WindowsServiceUtility;

namespace Averitt_RNA
{
    class RegionProcessor : Processor
    {

        private Region _Region;
        private ApexConsumer _ApexConsumer;
        private IntegrationDBAccessor _IntegrationDBAccessor;

        public RegionProcessor(Region region) : base(MethodBase.GetCurrentMethod().DeclaringType, region.Identifier)
        {
            _Region = region;
            _ApexConsumer = new ApexConsumer(region, Logger);
            _IntegrationDBAccessor = new IntegrationDBAccessor(Logger);
        }

        public override void Process()
        {

            if (!MainService.SessionRequired)
            {
                Logger.Debug("Start Retrieving");
                //TODO

            }
            else
            {
                Logger.Info("Waiting for Session.");
            }

        }

    }
}
