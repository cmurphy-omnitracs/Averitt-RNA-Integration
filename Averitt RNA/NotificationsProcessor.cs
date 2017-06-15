using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Averitt_RNA.Apex;
using WindowsServiceUtility;

namespace Averitt_RNA
{
    class NotificationsProcessor : Processor
    {

        private long _BusinessUnitEntityKey;
        private ApexConsumer _ApexConsumer;

        public NotificationsProcessor(long businessUnitEntityKey)
            : base(MethodBase.GetCurrentMethod().DeclaringType, businessUnitEntityKey.ToString())
        {
            _BusinessUnitEntityKey = businessUnitEntityKey;
            _ApexConsumer = new ApexConsumer(businessUnitEntityKey, Logger);
        }

        public override void Process()
        {

            if (!MainService.SessionRequired)
            {

                //TODO

            }
            else
            {
                Logger.Info("Waiting for Session.");
            }

        }

    }
}
