using System;
using System.Linq;
using System.ServiceModel;
using Averitt_RNA.Apex;
using System.Collections.Generic;
using System.IO;






namespace Averitt_RNA
{
    public class ApexConsumer
    {

        #region Private Members

        private long _BusinessUnitEntityKey;
        private Region _Region;
        private RegionContext _RegionContext;
        private log4net.ILog _Logger;
        private QueryServiceClient _QueryServiceClient;
        private MappingServiceClient _MappingServiceClient;
        private RoutingServiceClient _RoutingServiceClient;

        #endregion

        #region Public Members

        public enum ErrorLevel
        {
            None,
            Transient,
            Partial,
            Fatal
        }
        public enum TaskSpecType
        {
            None,
            Delivery,
            Pickup,
            DeliveryAndPickup,
            Transfer
        }
        public enum RetrieveType
        {
            CreateRoutesJobInfo,
            CustomForm,
            CustomFormResponse,
            DailyPass,
            DailyRoutingSession,
            Depot,
            Equipment,
            EquipmentManufacturer,
            EquipmentType,
            FormControl,
            MobileDevice,
            Notification,
            NotificationRecipient,
            Order,
            OrderClass,
            PackageType,
            Region,
            Route,
            ServiceLocation,
            ServiceTimeType,
            SKU,
            TelematicsDevice,
            TimeWindowType,
            UnassignedOrderGroup,
            Worker,
            WorkerType
        }

        public const string DATE_FORMAT = "yyyy-MM-dd";
        public const string DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss.fffffff";
        public const string TIMESPAN_FORMAT = "hh\\:mm\\:ss\\.fffffff";
        public const string TRUNCATED_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
        public const string CANADA_COUNTRY_ISO3_ABBR = "CAN";
        public const string MEXICO_COUNTRY_ISO3_ABBR = "MEX";
        public const string USA_COUNTRY_ISO3_ABBR = "USA";
        public const string NOTIFICATION_RECIPIENT_IDENTIFIER = "INTEGRATION";
        public const int BATCH_SIZE = 100;
        public const string DEFAULT_IDENTIFIER = "DEFAULT";
        public const int MAX_GEOCODE_RETRIES = 5;

        #endregion

        #region Public Static Methods

        public static string GenerateUniqueIdentifier()
        {
            return Guid.NewGuid().ToString().Replace("-", string.Empty).ToUpper();
        }

        public static OrderSpec ConvertOrderToOrderSpec(Order order)
        {
            OrderSpec orderSpec = new OrderSpec
            {
                BeginDate = order.BeginDate,
                CustomProperties = order.CustomProperties,
                EndDate = order.EndDate,
                ForceBulkServiceTime = order.ForceBulkServiceTime,
                Identifier = order.Identifier,
                LineItems = order.LineItems,
                ManagedByUserEntityKey = order.ManagedByUserEntityKey,
                NetRevenue = order.NetRevenue,
                OrderClassEntityKey = order.OrderClassEntityKey,
                OrderInstance = new DomainInstance
                {
                    EntityKey = order.EntityKey,
                    Version = order.Version
                },
                PlannedDeliveryCategory1Quantities = order.PlannedDeliveryCategory1Quantities,
                PlannedDeliveryCategory2Quantities = order.PlannedDeliveryCategory2Quantities,
                PlannedDeliveryCategory3Quantities = order.PlannedDeliveryCategory3Quantities,
                PlannedPickupCategory1Quantities = order.PlannedPickupCategory1Quantities,
                PlannedPickupCategory2Quantities = order.PlannedPickupCategory2Quantities,
                PlannedPickupCategory3Quantities = order.PlannedPickupCategory3Quantities,
                PreferredRouteIdentifierOverride = order.PreferredRouteIdentifierOverride,
                RegionEntityKey = order.RegionEntityKey,
                Selector = order.Selector,
                SessionEntityKey = order.SessionEntityKey,
                SpecialInstructions = order.SpecialInstructions,
                TakenBy = order.TakenBy,
                UploadSelector = order.UploadSelector
            };
            TaskSpecType taskSpecType = TaskSpecType.None;
            if (order.LineItems == null || order.LineItems.Length == 0)
            {
                if (order.Tasks != null && order.Tasks.Length != 0)
                {
                    if (order.Tasks.All(task => task.TaskType_Type == Enum.GetName(typeof(TaskType), TaskType.Delivery)))
                    {
                        taskSpecType = TaskSpecType.Delivery;
                    }
                    else if (order.Tasks.All(task => task.TaskType_Type == Enum.GetName(typeof(TaskType), TaskType.Pickup)))
                    {
                        taskSpecType = TaskSpecType.Pickup;
                    }
                    else
                    {
                        long firstTaskLocationEntityKey = order.Tasks.First().LocationEntityKey;
                        if (order.Tasks.All(task => task.LocationEntityKey == firstTaskLocationEntityKey))
                        {
                            taskSpecType = TaskSpecType.DeliveryAndPickup;
                        }
                        else
                        {
                            taskSpecType = TaskSpecType.Transfer;
                        }
                    }
                }
            }
            else
            {
                if (order.LineItems.All(lineItem => lineItem.LineItemType_Type == Enum.GetName(typeof(LineItemType), LineItemType.Delivery)))
                {
                    taskSpecType = TaskSpecType.Delivery;
                }
                else if (order.LineItems.All(lineItem => lineItem.LineItemType_Type == Enum.GetName(typeof(LineItemType), LineItemType.Pickup)))
                {
                    taskSpecType = TaskSpecType.Pickup;
                }
                else if (order.LineItems.All(lineItem => lineItem.LineItemType_Type == Enum.GetName(typeof(LineItemType), LineItemType.Transfer)))
                {
                    taskSpecType = TaskSpecType.Transfer;
                }
                else
                {
                    taskSpecType = TaskSpecType.DeliveryAndPickup;
                }
            }
            Task deliveryTask;
            Task pickupTask;
            switch (taskSpecType)
            {
                case TaskSpecType.Delivery:
                    orderSpec.TaskSpec = new DeliveryTaskSpec
                    {
                        AdditionalServiceTime = order.AdditionalServiceTime,
                        Quantities = order.PlannedDeliveryQuantities,
                        RequiredOriginEntityKey = order.RequiredRouteOriginEntityKey
                    };
                    deliveryTask = order.Tasks.FirstOrDefault();
                    if (deliveryTask != null)
                    {
                        ((DeliveryTaskSpec)orderSpec.TaskSpec).CoordinateOverride = deliveryTask.CoordinateOverride;
                        ((DeliveryTaskSpec)orderSpec.TaskSpec).OpenCloseOverrides = deliveryTask.OpenCloseOverrides;
                        ((DeliveryTaskSpec)orderSpec.TaskSpec).ServiceLocationEntityKey = deliveryTask.LocationEntityKey;
                        ((DeliveryTaskSpec)orderSpec.TaskSpec).ServiceWindowOverrides = deliveryTask.ServiceWindowOverrides;
                    }
                    break;
                case TaskSpecType.Pickup:
                    orderSpec.TaskSpec = new PickupTaskSpec
                    {
                        AdditionalServiceTime = order.AdditionalServiceTime,
                        Quantities = order.PlannedPickupQuantities,
                        RequiredDestinationEntityKey = order.RequiredRouteDestinationEntityKey
                    };
                    pickupTask = order.Tasks.FirstOrDefault();
                    if (pickupTask != null)
                    {
                        ((PickupTaskSpec)orderSpec.TaskSpec).CoordinateOverride = pickupTask.CoordinateOverride;
                        ((PickupTaskSpec)orderSpec.TaskSpec).OpenCloseOverrides = pickupTask.OpenCloseOverrides;
                        ((PickupTaskSpec)orderSpec.TaskSpec).ServiceLocationEntityKey = pickupTask.LocationEntityKey;
                        ((PickupTaskSpec)orderSpec.TaskSpec).ServiceWindowOverrides = pickupTask.ServiceWindowOverrides;
                    }
                    break;
                case TaskSpecType.DeliveryAndPickup:
                    deliveryTask = order.Tasks[0];
                    pickupTask = order.Tasks[1];
                    orderSpec.TaskSpec = new DeliveryAndPickupTaskSpec
                    {
                        AdditionalServiceTime = deliveryTask.AdditionalServiceTime,
                        CoordinateOverride = deliveryTask.CoordinateOverride,
                        DeliveryQuantities = deliveryTask.PlannedQuantities,
                        OpenCloseOverrides = deliveryTask.OpenCloseOverrides,
                        PickupQuantities = pickupTask.PlannedQuantities,
                        RequiredDestinationEntityKey = order.RequiredRouteDestinationEntityKey,
                        RequiredOriginEntityKey = order.RequiredRouteOriginEntityKey,
                        ServiceLocationEntityKey = deliveryTask.LocationEntityKey,
                        ServiceWindowOverrides = deliveryTask.ServiceWindowOverrides
                    };
                    break;
                case TaskSpecType.Transfer:
                    deliveryTask = order.Tasks[1];
                    pickupTask = order.Tasks[0];
                    orderSpec.TaskSpec = new TransferTaskSpec
                    {
                        DeliveryAdditionalServiceTime = TimeSpan.FromSeconds(order.AdditionalServiceTime.TotalSeconds / 2),
                        DeliveryCoordinateOverride = deliveryTask.CoordinateOverride,
                        DeliveryLocationEntityKey = deliveryTask.LocationEntityKey,
                        DeliveryOpenCloseOverrides = deliveryTask.OpenCloseOverrides,
                        DeliveryServiceWindowOverrides = deliveryTask.ServiceWindowOverrides,
                        PickupAdditionalServiceTime = TimeSpan.FromSeconds(order.AdditionalServiceTime.TotalSeconds / 2),
                        PickupCoordinateOverride = pickupTask.CoordinateOverride,
                        PickupLocationEntityKey = pickupTask.LocationEntityKey,
                        PickupOpenCloseOverrides = pickupTask.OpenCloseOverrides,
                        PickupServiceWindowOverrides = pickupTask.ServiceWindowOverrides,
                        Quantities = order.PlannedDeliveryQuantities,
                        RequiredDestinationEntityKey = order.RequiredRouteDestinationEntityKey,
                        RequiredOriginEntityKey = order.RequiredRouteOriginEntityKey
                    };
                    break;
            }
            return orderSpec;
        }

        public static string ToString(Address address)
        {
            return string.Format("{0} | {1} | {2}",
                address.AddressLine1,
                address.AddressLine2,
                address.Locality != null ? ToString(address.Locality) : string.Empty);
        }

        public static string ToString(Coordinate coordinate)
        {
            return string.Format("{0} | {1}",
                coordinate.Latitude,
                coordinate.Longitude);
        }

        public static string ToString(CreateRouteArgs createRouteArgs)
        {
            return string.Format("{0} | {1}",
                createRouteArgs.RouteArgs != null ? ToString(createRouteArgs.RouteArgs) : string.Empty,
                createRouteArgs.Stops != null ? string.Join(" | ", createRouteArgs.Stops.Select(saveStopArgs => ToString(saveStopArgs))) : string.Empty);
        }

        public static string ToString(CreateRoutesResult.CreateRouteResult.CreateRouteErrors createRouteErrors)
        {
            return string.Format("{0} | {1} | {2} | {3}",
                createRouteErrors.RouteIdentifier,
                createRouteErrors.StopIndex,
                createRouteErrors.OrderIdentifier,
                createRouteErrors.Failure != null ? ToString(createRouteErrors.Failure) : string.Empty);
        }

        public static string ToString(CreateRoutesResult.CreateRouteResult createRouteResult)
        {
            return string.Format("{0} | {1}",
                createRouteResult.RouteEntityKey,
                createRouteResult.Errors != null ? string.Join(" | ", createRouteResult.Errors.Select(error => ToString(error))) : string.Empty);
        }

        public static string ToString(CreateRoutesJobInfo createRoutesJobInfo)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6}",
                createRoutesJobInfo.EntityKey,
                createRoutesJobInfo.JobState_State,
                createRoutesJobInfo.StartTime.HasValue ? createRoutesJobInfo.StartTime.Value.ToString(DATETIME_FORMAT) : string.Empty,
                createRoutesJobInfo.FinishTime.HasValue ? createRoutesJobInfo.FinishTime.Value.ToString(DATETIME_FORMAT) : string.Empty,
                createRoutesJobInfo.PercentComplete,
                createRoutesJobInfo.Result != null ? ToString(createRoutesJobInfo.Result) : string.Empty,
                createRoutesJobInfo.JobFailureReason_FailureReason);
        }

        public static string ToString(CreateRoutesResult createRoutesResult)
        {
            return string.Format("{0} | {1} | {2} | {3}",
                createRoutesResult.RoutesSubmitted,
                createRoutesResult.RoutesCreated,
                createRoutesResult.RoutesFailed,
                createRoutesResult.RouteResults != null ? string.Join(" | ", createRoutesResult.RouteResults.Select(routeResult => ToString(routeResult))) : string.Empty);
        }

        public static string ToString(CustomFormResponse customFormResponse)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8} | {9} | {10}",
                customFormResponse.RegionEntityKey,
                customFormResponse.EntityKey,
                customFormResponse.CustomFormEntityKey,
                customFormResponse.CustomFormIdentifier,
                customFormResponse.PerformedAt_PerformedAt,
                customFormResponse.EquipmentEntityKey,
                customFormResponse.RouteEntityKey,
                customFormResponse.StopEntityKey.HasValue ? customFormResponse.StopEntityKey.Value.ToString() : string.Empty,
                customFormResponse.OrderEntityKey.HasValue ? customFormResponse.OrderEntityKey.Value.ToString() : string.Empty,
                customFormResponse.LineItemEntityKey.HasValue ? customFormResponse.LineItemEntityKey.Value.ToString() : string.Empty,
                customFormResponse.Responses != null ? string.Join(" | ", customFormResponse.Responses.Select(response => ToString(response))) : string.Empty);
        }

        public static string ToString(DailyPass dailyPass)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5}",
                dailyPass.RegionEntityKey,
                dailyPass.SessionEntityKey,
                dailyPass.EntityKey,
                dailyPass.Identifier,
                dailyPass.Description,
                dailyPass.PassTemplateEntityKey);
        }

        public static string ToString(DailyRoutingSession dailyRoutingSession)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6}",
                dailyRoutingSession.RegionEntityKey,
                dailyRoutingSession.EntityKey,
                dailyRoutingSession.StartDate,
                dailyRoutingSession.Description,
                dailyRoutingSession.SessionMode_Mode,
                dailyRoutingSession.TimeUnit_TimeUnitType,
                dailyRoutingSession.NumberOfTimeUnits);
        }

        public static string ToString(DailyTimePeriod dailyTimePeriod)
        {
            return string.Format("{0} | {1} | {2} | {3}",
                dailyTimePeriod.DayOfWeekFlags_DaysOfWeek,
                dailyTimePeriod.StartTime,
                dailyTimePeriod.EndTime,
                dailyTimePeriod.TimeSpan);
        }

        public static string ToString(DeliveryAndPickupTaskSpec deliveryAndPickupTaskSpec)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6}",
                deliveryAndPickupTaskSpec.ServiceLocationEntityKey,
                deliveryAndPickupTaskSpec.RequiredOriginEntityKey,
                deliveryAndPickupTaskSpec.RequiredDestinationEntityKey,
                deliveryAndPickupTaskSpec.DeliveryQuantities != null ? ToString(deliveryAndPickupTaskSpec.DeliveryQuantities) : string.Empty,
                deliveryAndPickupTaskSpec.PickupQuantities != null ? ToString(deliveryAndPickupTaskSpec.PickupQuantities) : string.Empty,
                deliveryAndPickupTaskSpec.OpenCloseOverrides != null ? string.Join(" | ", deliveryAndPickupTaskSpec.OpenCloseOverrides.Select(openCloseOverride => ToString(openCloseOverride))) : string.Empty,
                deliveryAndPickupTaskSpec.ServiceWindowOverrides != null ? string.Join(" | ", deliveryAndPickupTaskSpec.ServiceWindowOverrides.Select(serviceWindowOverride => ToString(serviceWindowOverride))) : string.Empty);
        }

        public static string ToString(DeliveryTaskSpec deliveryTaskSpec)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4}",
                deliveryTaskSpec.ServiceLocationEntityKey,
                deliveryTaskSpec.RequiredOriginEntityKey,
                deliveryTaskSpec.Quantities != null ? ToString(deliveryTaskSpec.Quantities) : string.Empty,
                deliveryTaskSpec.OpenCloseOverrides != null ? string.Join(" | ", deliveryTaskSpec.OpenCloseOverrides.Select(openCloseOverride => ToString(openCloseOverride))) : string.Empty,
                deliveryTaskSpec.ServiceWindowOverrides != null ? string.Join(" | ", deliveryTaskSpec.ServiceWindowOverrides.Select(serviceWindowOverride => ToString(serviceWindowOverride))) : string.Empty);
        }

        public static string ToString(Depot depot)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7}",
                depot.EntityKey,
                depot.Identifier,
                depot.BusinessUnitEntityKey,
                depot.CreatedInRegionEntityKey,
                depot.RegionEntityKeys != null ? string.Join(" | ", depot.RegionEntityKeys) : string.Empty,
                depot.VisibleInAllRegions,
                depot.Description,
                depot.Address != null ? ToString(depot.Address) : string.Empty);
        }

        public static string ToString(DomainInstance domainInstance)
        {
            return string.Format("{0} | {1}",
                domainInstance.EntityKey,
                domainInstance.Version);
        }

        public static string ToString(Equipment equipment)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8} | {9} | {10} | {11}",
                equipment.EntityKey,
                equipment.Identifier,
                equipment.BusinessUnitEntityKey,
                equipment.CreatedInRegionEntityKey,
                equipment.RegionEntityKeys != null ? string.Join(" | ", equipment.RegionEntityKeys) : string.Empty,
                equipment.VisibleInAllRegions,
                equipment.Description,
                equipment.EquipmentTypeEntityKey,
                equipment.IsActive,
                equipment.DepotEntityKey.HasValue ? equipment.DepotEntityKey.Value.ToString() : string.Empty,
                equipment.PreferredWorkerEntityKey.HasValue ? equipment.PreferredWorkerEntityKey.Value.ToString() : string.Empty,
                equipment.TelematicsDeviceEntityKey.HasValue ? equipment.TelematicsDeviceEntityKey.Value.ToString() : string.Empty);
        }

        public static string ToString(EquipmentType equipmentType)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8}",
                equipmentType.EntityKey,
                equipmentType.Identifier,
                equipmentType.BusinessUnitEntityKey,
                equipmentType.CreatedInRegionEntityKey,
                equipmentType.RegionEntityKeys != null ? string.Join(" | ", equipmentType.RegionEntityKeys) : string.Empty,
                equipmentType.VisibleInAllRegions,
                equipmentType.Description,
                equipmentType.EquipmentClassification_Classification,
                equipmentType.PowerUnit);
        }

        public static string ToString(FormControlResponse formControlResponse)
        {
            return string.Format("{0} | {1} | {2} | {3}",
                formControlResponse.EntityKey,
                formControlResponse.FormControlEntityKey,
                formControlResponse.AnsweredTime.HasValue ? formControlResponse.AnsweredTime.Value.ToString(DATETIME_FORMAT) : string.Empty,
                formControlResponse.Value != null ? ToString(formControlResponse.Value) : string.Empty);
        }

        public static string ToString(FormControlResponseBinaryValue formControlResponseBinaryValue)
        {
            return string.Format("{0} | {1} | {2}",
                formControlResponseBinaryValue.BinaryDataType,
                formControlResponseBinaryValue.BinaryValue != null ? formControlResponseBinaryValue.BinaryValue.Length.ToString() : string.Empty,
                formControlResponseBinaryValue.TextValue);
        }

        public static string ToString(FormControlResponseBooleanValue formControlResponseBooleanValue)
        {
            return formControlResponseBooleanValue.BooleanValue.HasValue ? formControlResponseBooleanValue.BooleanValue.Value.ToString() : string.Empty;
        }

        public static string ToString(FormControlResponseDateTimeValue formControlResponseDateTimeValue)
        {
            return formControlResponseDateTimeValue.DateTimeValue.HasValue ? formControlResponseDateTimeValue.DateTimeValue.Value.ToString() : string.Empty;
        }

        public static string ToString(FormControlResponseInspectionValue formControlResponseInspectionValue)
        {
            return string.Format("{0} | {1}",
                formControlResponseInspectionValue.IsPositive,
                formControlResponseInspectionValue.TextValue);
        }

        public static string ToString(FormControlResponseNumericValue formControlResponseNumericValue)
        {
            return formControlResponseNumericValue.NumericValue.HasValue ? formControlResponseNumericValue.NumericValue.Value.ToString() : string.Empty;
        }

        public static string ToString(FormControlResponseTextValue formControlResponseTextValue)
        {
            return formControlResponseTextValue.TextValue;
        }

        public static string ToString(FormControlResponseValue formControlResponseValue)
        {
            if (formControlResponseValue is FormControlResponseBinaryValue)
            {
                return ToString((FormControlResponseBinaryValue)formControlResponseValue);
            }
            else if (formControlResponseValue is FormControlResponseBooleanValue)
            {
                return ToString((FormControlResponseBooleanValue)formControlResponseValue);
            }
            else if (formControlResponseValue is FormControlResponseDateTimeValue)
            {
                return ToString((FormControlResponseDateTimeValue)formControlResponseValue);
            }
            else if (formControlResponseValue is FormControlResponseInspectionValue)
            {
                return ToString((FormControlResponseInspectionValue)formControlResponseValue);
            }
            else if (formControlResponseValue is FormControlResponseNumericValue)
            {
                return ToString((FormControlResponseNumericValue)formControlResponseValue);
            }
            else if (formControlResponseValue is FormControlResponseTextValue)
            {
                return ToString((FormControlResponseTextValue)formControlResponseValue);
            }
            else
            {
                return formControlResponseValue.ToString();
            }
        }

        public static string ToString(GeocodeCandidate geocodeCandidate)
        {
            return string.Format("{0} | {1} | {2}",
                geocodeCandidate.Coordinate != null ? ToString(geocodeCandidate.Coordinate) : string.Empty,
                geocodeCandidate.GeocodeAccuracy_Quality,
                geocodeCandidate.Score);
        }

        public static string ToString(LayoverDeparture layoverDeparture)
        {
            return layoverDeparture.ToString();
        }

        public static string ToString(LineItem lineItem)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7}",
                lineItem.EntityKey,
                lineItem.Identifier,
                lineItem.LineItemType_Type,
                lineItem.SKUEntityKey,
                lineItem.SKUIdentifier,
                lineItem.PlannedQuantities != null ? ToString(lineItem.PlannedQuantities) : string.Empty,
                lineItem.Quantities != null ? ToString(lineItem.Quantities) : string.Empty,
                lineItem.QuantitiesReasonCode != null ? ToString(lineItem.QuantitiesReasonCode) : string.Empty);
        }

        public static string ToString(Locality locality)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6}",
                locality.CountryISO3Abbr,
                locality.PostalCode,
                locality.AdminDivision1,
                locality.AdminDivision2,
                locality.AdminDivision3,
                locality.AdminDivision4,
                locality.AdminDivision5);
        }

        public static string ToString(ManipulationResult.ManipulationError manipulationError)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5}",
                manipulationError.RouteEntityKey,
                manipulationError.LocationEntityKey,
                manipulationError.StopEntityKey.HasValue ? manipulationError.StopEntityKey.Value.ToString() : string.Empty,
                manipulationError.OrderEntityKey.HasValue ? manipulationError.OrderEntityKey.Value.ToString() : string.Empty,
                manipulationError.Reason != null ? ToString(manipulationError.Reason) : string.Empty,
                manipulationError.MatchingPatternEntityKeys != null ? string.Join(" | ", manipulationError.MatchingPatternEntityKeys) : string.Empty);
        }

        public static string ToString(MobileDevice mobileDevice)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8} | {9}",
                mobileDevice.EntityKey,
                mobileDevice.Identifier,
                mobileDevice.BusinessUnitEntityKey,
                mobileDevice.RegionEntityKeys != null ? string.Join(" | ", mobileDevice.RegionEntityKeys) : string.Empty,
                mobileDevice.Active,
                mobileDevice.DevicePhoneNumber,
                mobileDevice.EquipmentEntityKey.HasValue ? mobileDevice.EquipmentEntityKey.Value.ToString() : string.Empty,
                mobileDevice.MobileNetworkOperator_MobileNetworkOperator,
                mobileDevice.MobilePlatform_MobilePlatform,
                mobileDevice.VisibleInAllRegions);
        }

        public static string ToString(NamedPlace namedPlace)
        {
            return string.Format("{0} | {1}",
                namedPlace.PlaceName,
                namedPlace.PlaceAddress != null ? ToString(namedPlace.PlaceAddress) : string.Empty);
        }

        public static string ToString(Notification notification)
        {
            if (notification is RoutePhaseChangeNotification)
            {
                return ToString((RoutePhaseChangeNotification)notification);
            }
            else if (notification is RouteStateChangeNotification)
            {
                return ToString((RouteStateChangeNotification)notification);
            }
            else if (notification is StopSequenceNotification)
            {
                return ToString((StopSequenceNotification)notification);
            }
            else if (notification is StopStateChangeNotification)
            {
                return ToString((StopStateChangeNotification)notification);
            }
            else
            {
                return string.Format("{0} | {1} | {2} | {3} | {4}",
                    notification.EntityKey,
                    notification.NotificationType_NotificationType,
                    notification.RecipientEntityKey,
                    notification.RouteEntityKey.HasValue ? notification.RouteEntityKey.Value.ToString() : string.Empty,
                    notification.TelematicsDeviceEntityKey.HasValue ? notification.TelematicsDeviceEntityKey.Value.ToString() : string.Empty);
            }
        }

        public static string ToString(Order order)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8} | {9} | {10} | {11} | {12} | {13} | {14} | {15} | {16}",
                order.RegionEntityKey,
                order.Identifier,
                order.OrderType_Type,
                order.OrderState_State,
                order.EntryMethod_EntryMethod,
                order.SessionEntityKey,
                order.BeginDate,
                order.EndDate,
                order.OrderClassEntityKey,
                order.ServiceTime,
                order.RequiredRouteOriginEntityKey,
                order.RequiredRouteDestinationEntityKey,
                order.PlannedDeliveryQuantities != null ? ToString(order.PlannedDeliveryQuantities) : string.Empty,
                order.PlannedPickupQuantities != null ? ToString(order.PlannedPickupQuantities) : string.Empty,
                order.DeliveryQuantities != null ? ToString(order.DeliveryQuantities) : string.Empty,
                order.PickupQuantities != null ? ToString(order.PickupQuantities) : string.Empty,
                order.LineItems != null ? string.Join(" | ", order.LineItems.Select(lineItem => ToString(lineItem))) : string.Empty);
        }

        public static string ToString(OrderSpec orderSpec)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7}",
                orderSpec.RegionEntityKey,
                orderSpec.Identifier,
                orderSpec.SessionEntityKey,
                orderSpec.BeginDate,
                orderSpec.EndDate,
                orderSpec.OrderClassEntityKey,
                orderSpec.TaskSpec != null
                    ? (orderSpec.TaskSpec is DeliveryAndPickupTaskSpec
                        ? ToString((DeliveryAndPickupTaskSpec)orderSpec.TaskSpec)
                        : (orderSpec.TaskSpec is DeliveryTaskSpec
                            ? ToString((DeliveryTaskSpec)orderSpec.TaskSpec)
                            : (orderSpec.TaskSpec is PickupTaskSpec
                                ? ToString((PickupTaskSpec)orderSpec.TaskSpec)
                                : ToString((TransferTaskSpec)orderSpec.TaskSpec))))
                    : string.Empty,
                orderSpec.LineItems != null ? string.Join(" | ", orderSpec.LineItems.Select(lineItem => ToString(lineItem))) : string.Empty);
        }

        public static string ToString(PersonName personName)
        {
            return string.Format("{0} | {1} | {2}",
                personName.First,
                personName.Middle,
                personName.Last);
        }

        public static string ToString(PickupTaskSpec pickupTaskSpec)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4}",
                pickupTaskSpec.ServiceLocationEntityKey,
                pickupTaskSpec.RequiredDestinationEntityKey,
                pickupTaskSpec.Quantities != null ? ToString(pickupTaskSpec.Quantities) : string.Empty,
                pickupTaskSpec.OpenCloseOverrides != null ? string.Join(" | ", pickupTaskSpec.OpenCloseOverrides.Select(openCloseOverride => ToString(openCloseOverride))) : string.Empty,
                pickupTaskSpec.ServiceWindowOverrides != null ? string.Join(" | ", pickupTaskSpec.ServiceWindowOverrides.Select(serviceWindowOverride => ToString(serviceWindowOverride))) : string.Empty);
        }

        public static string ToString(QualityPairedDateTime qualityPairedDateTime)
        {
            return string.Format("{0} | {1}",
                qualityPairedDateTime.Value,
                qualityPairedDateTime.DataQuality_Quality);
        }

        public static string ToString(Quantities quantities)
        {
            return string.Format("{0} | {1} | {2}",
                quantities.Size1,
                quantities.Size2,
                quantities.Size3);
        }

        public static string ToString(QuantityReasonCode quantityReasonCode)
        {
            return string.Format("{0} | {1} | {2}",
                quantityReasonCode.EntityKey,
                quantityReasonCode.Identifier,
                quantityReasonCode.QuantityType_QuantityTypes);
        }

        public static string ToString(Region region)
        {
            return string.Format("{0} | {1} | {2}",
                region.BusinessUnitEntityKey,
                region.EntityKey,
                region.Identifier);
        }

        public static string ToString(Route route)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8} | {9} | {10} | {11} | {12} | {13} | {14} | {15} | {16} | {17} | {18} | {19} | {20}",
                route.EntityKey,
                route.Identifier,
                route.Description,
                route.RegionEntityKey,
                route.RoutePhase_Phase,
                route.RouteState_State,
                route.RoutingSessionDate,
                route.RoutingSessionEntityKey,
                route.IsLoadedToDevice,
                route.LastStopIsDestination,
                route.OriginDepotEntityKey,
                route.OriginDepotIdentifier,
                route.DestinationDepotEntityKey,
                route.DestinationDepotIdentifier,
                route.ArrivalTime != null ? ToString(route.ArrivalTime) : string.Empty,
                route.CompleteTime != null ? ToString(route.CompleteTime) : string.Empty,
                route.DepartureTime != null ? ToString(route.DepartureTime) : string.Empty,
                route.StartTime != null ? ToString(route.StartTime) : string.Empty,
                route.Equipment != null ? string.Join(" | ", route.Equipment.Select(equipment => ToString(equipment))) : string.Empty,
                route.Workers != null ? string.Join(" | ", route.Workers.Select(worker => ToString(worker))) : string.Empty,
                route.Stops != null ? string.Join(" | ", route.Stops.Select(stop => ToString(stop))) : string.Empty);
        }

        public static string ToString(RouteEquipment routeEquipment)
        {
            return string.Format("{0} | {1} | {2} | {3}",
                routeEquipment.EntityKey,
                routeEquipment.EquipmentEntityKey,
                routeEquipment.EquipmentIdentifier,
                routeEquipment.EquipmentDescription);
        }

        public static string ToString(RouteEquipmentBase routeEquipmentBase)
        {
            if (routeEquipmentBase is RouteEquipment)
            {
                return ToString((RouteEquipment)routeEquipmentBase);
            }
            else if (routeEquipmentBase is RouteEquipmentType)
            {
                return ToString((RouteEquipmentType)routeEquipmentBase);
            }
            else
            {
                return routeEquipmentBase.EntityKey.ToString();
            }
        }

        public static string ToString(RouteEquipmentType routeEquipmentType)
        {
            return string.Format("{0} | {1}",
                routeEquipmentType.EntityKey,
                routeEquipmentType.EquipmentTypeEntityKey);
        }

        public static string ToString(RoutePhaseChangeNotification routePhaseChangeNotification)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5}",
                routePhaseChangeNotification.EntityKey,
                routePhaseChangeNotification.NotificationType_NotificationType,
                routePhaseChangeNotification.RecipientEntityKey,
                routePhaseChangeNotification.RouteEntityKey.HasValue ? routePhaseChangeNotification.RouteEntityKey.Value.ToString() : string.Empty,
                routePhaseChangeNotification.TelematicsDeviceEntityKey.HasValue ? routePhaseChangeNotification.TelematicsDeviceEntityKey.Value.ToString() : string.Empty,
                routePhaseChangeNotification.RoutePhase_Phase);
        }

        public static string ToString(RouteStateChangeNotification routeStateChangeNotification)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5}",
                routeStateChangeNotification.EntityKey,
                routeStateChangeNotification.NotificationType_NotificationType,
                routeStateChangeNotification.RecipientEntityKey,
                routeStateChangeNotification.RouteEntityKey.HasValue ? routeStateChangeNotification.RouteEntityKey.Value.ToString() : string.Empty,
                routeStateChangeNotification.TelematicsDeviceEntityKey.HasValue ? routeStateChangeNotification.TelematicsDeviceEntityKey.Value.ToString() : string.Empty,
                routeStateChangeNotification.RouteState_State);
        }

        public static string ToString(RouteWorker routeWorker)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4}",
                routeWorker.EntityKey,
                routeWorker.Index,
                routeWorker.WorkerEntityKey,
                routeWorker.WorkerIdentifier,
                routeWorker.WorkerName != null ? ToString(routeWorker.WorkerName) : string.Empty);
        }

        public static string ToString(SaveAssignedOrderArgs saveAssignedOrderArgs)
        {
            return string.Format("{0} | {1} | {2}",
                saveAssignedOrderArgs.Identifier,
                saveAssignedOrderArgs.OrderClassEntityKey,
                saveAssignedOrderArgs.DeliveryQuantities != null ? ToString(saveAssignedOrderArgs.DeliveryQuantities) : string.Empty,
                saveAssignedOrderArgs.PickupQuantities != null ? ToString(saveAssignedOrderArgs.PickupQuantities) : string.Empty,
                saveAssignedOrderArgs.LineItems != null ? string.Join(" | ", saveAssignedOrderArgs.LineItems.Select(lineItem => ToString(lineItem))) : string.Empty);
        }

        public static string ToString(SaveErrorInfo saveErrorInfo)
        {
            return string.Format("{0} | {1} | {2}",
                saveErrorInfo.Code != null ? saveErrorInfo.Code.ErrorCode_Status : string.Empty,
                saveErrorInfo.Detail,
                saveErrorInfo.ValidationFailures != null ? string.Join(" | ", saveErrorInfo.ValidationFailures.Select(validationFailure => ToString(validationFailure))) : string.Empty);
        }

        public static string ToString(SaveLayoverStopArgs saveLayoverStopArgs)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6}",
                saveLayoverStopArgs.EntityKey,
                saveLayoverStopArgs.Coordinate != null ? ToString(saveLayoverStopArgs.Coordinate) : string.Empty,
                saveLayoverStopArgs.Departure != null ? ToString(saveLayoverStopArgs.Departure) : string.Empty,
                saveLayoverStopArgs.LayoverLocationEntityKey.HasValue ? saveLayoverStopArgs.LayoverLocationEntityKey.Value.ToString() : string.Empty,
                saveLayoverStopArgs.Paid,
                saveLayoverStopArgs.PlacementType_PlacementType,
                Enum.GetName(typeof(WorldTimeZone), saveLayoverStopArgs.TimeZone));
        }

        public static string ToString(SaveMidRouteDepotStopArgs saveMidRouteDepotStopArgs)
        {
            return string.Format("{0} | {1} | {2} | {3}",
                saveMidRouteDepotStopArgs.EntityKey,
                saveMidRouteDepotStopArgs.DepotEntityKey,
                saveMidRouteDepotStopArgs.LoadAction.HasValue ? Enum.GetName(typeof(LoadAction), saveMidRouteDepotStopArgs.LoadAction.Value) : string.Empty,
                saveMidRouteDepotStopArgs.ReloadTimeOverride.HasValue ? saveMidRouteDepotStopArgs.ReloadTimeOverride.Value.ToString(TIMESPAN_FORMAT) : string.Empty);
        }

        public static string ToString(SaveNonServiceableStopArgs saveNonServiceableStopArgs)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5}",
                saveNonServiceableStopArgs.EntityKey,
                saveNonServiceableStopArgs.Coordinate != null ? ToString(saveNonServiceableStopArgs.Coordinate) : string.Empty,
                saveNonServiceableStopArgs.Duration != null ? saveNonServiceableStopArgs.Duration.ToString(TIMESPAN_FORMAT) : string.Empty,
                saveNonServiceableStopArgs.Paid,
                Enum.GetName(typeof(PlacementType), saveNonServiceableStopArgs.PlacementType),
                Enum.GetName(typeof(WorldTimeZone), saveNonServiceableStopArgs.TimeZone));
        }

        public static string ToString(SaveRouteArgs saveRouteArgs)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8}",
                saveRouteArgs.Identifier,
                saveRouteArgs.Description,
                Enum.GetName(typeof(RoutePhase), saveRouteArgs.Phase),
                saveRouteArgs.OriginDepotEntityKey,
                saveRouteArgs.DestinationEntityKey.HasValue ? saveRouteArgs.DestinationEntityKey.Value.ToString() : string.Empty,
                saveRouteArgs.LastStopIsDestination,
                saveRouteArgs.StartTime != null ? saveRouteArgs.StartTime.ToString(DATETIME_FORMAT) : string.Empty,
                saveRouteArgs.Workers != null ? string.Join(" | ", saveRouteArgs.Workers.Select(worker => ToString(worker))) : string.Empty,
                saveRouteArgs.Equipment != null ? string.Join(" | ", saveRouteArgs.Equipment.Select(equipment => ToString(equipment))) : string.Empty);
        }

        public static string ToString(SaveServiceableStopArgs saveServiceableStopArgs)
        {
            return string.Format("{0} | {1} | {2}",
                saveServiceableStopArgs.ServiceLocationEntityKey,
                saveServiceableStopArgs.CoordinateOverride != null ? ToString(saveServiceableStopArgs.CoordinateOverride) : string.Empty,
                saveServiceableStopArgs.Orders != null ? string.Join(" | ", saveServiceableStopArgs.Orders.Select(order => ToString(order))) : string.Empty);
        }

        public static string ToString(SaveStopArgs saveStopArgs)
        {
            if (saveStopArgs is SaveLayoverStopArgs)
            {
                return ToString((SaveLayoverStopArgs)saveStopArgs);
            }
            else if (saveStopArgs is SaveMidRouteDepotStopArgs)
            {
                return ToString((SaveMidRouteDepotStopArgs)saveStopArgs);
            }
            else if (saveStopArgs is SaveNonServiceableStopArgs)
            {
                return ToString((SaveNonServiceableStopArgs)saveStopArgs);
            }
            if (saveStopArgs is SaveServiceableStopArgs)
            {
                return ToString((SaveServiceableStopArgs)saveStopArgs);
            }
            else
            {
                return saveStopArgs.ToString();
            }
        }

        public static string ToString(ServiceableStop serviceableStop)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8} | {9} | {10} | {11} | {12}",
                serviceableStop.EntityKey,
                serviceableStop.ServiceLocationEntityKey,
                serviceableStop.ServiceLocationIdentifier,
                serviceableStop.StopState_State,
                serviceableStop.IsCancelled,
                serviceableStop.IsUnserviceable,
                serviceableStop.RouteIdentifier,
                serviceableStop.SequenceNumber,
                serviceableStop.Index,
                serviceableStop.ArrivalTime != null ? ToString(serviceableStop.ArrivalTime) : string.Empty,
                serviceableStop.DepartureTime != null ? ToString(serviceableStop.DepartureTime) : string.Empty,
                serviceableStop.HasSignature,
                serviceableStop.Actions != null ? string.Join(" | ", serviceableStop.Actions.Select(action => ToString(action))) : string.Empty);
        }

        public static string ToString(ServiceLocation serviceLocation)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7}",
                serviceLocation.EntityKey,
                serviceLocation.Identifier,
                serviceLocation.BusinessUnitEntityKey,
                serviceLocation.CreatedInRegionEntityKey,
                serviceLocation.RegionEntityKeys != null ? string.Join(" | ", serviceLocation.RegionEntityKeys) : string.Empty,
                serviceLocation.VisibleInAllRegions,
                serviceLocation.Description,
                serviceLocation.Address != null ? ToString(serviceLocation.Address) : string.Empty);
        }

        public static string ToString(ServiceTimeDetail serviceTimeDetail)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8} | {9}",
                serviceTimeDetail.EntityKey,
                serviceTimeDetail.DayOfWeekFlags_Days,
                serviceTimeDetail.FixedNonHelper.ToString(TIMESPAN_FORMAT),
                serviceTimeDetail.PerUnitVariableNonHelper.ToString(TIMESPAN_FORMAT),
                serviceTimeDetail.BulkFixedNonHelper.ToString(TIMESPAN_FORMAT),
                serviceTimeDetail.PerUnitBulkVariableNonHelper.ToString(TIMESPAN_FORMAT),
                serviceTimeDetail.FixedHelper.ToString(TIMESPAN_FORMAT),
                serviceTimeDetail.PerUnitVariableHelper.ToString(TIMESPAN_FORMAT),
                serviceTimeDetail.BulkFixedHelper.ToString(TIMESPAN_FORMAT),
                serviceTimeDetail.PerUnitBulkVariableHelper.ToString(TIMESPAN_FORMAT));
        }

        public static string ToString(SKU sku)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6}",
                sku.EntityKey,
                sku.Identifier,
                sku.Description,
                sku.PackageTypeEntityKey,
                sku.PackageTypeIdentifier,
                sku.PackageTypeDescription,
                sku.ServiceTimeDetails != null ? string.Join(" | ", sku.ServiceTimeDetails.Select(serviceTimeDetail => ToString(serviceTimeDetail))) : string.Empty);
        }

        public static string ToString(Stop stop)
        {
            if (stop is ServiceableStop)
            {
                return ToString((ServiceableStop)stop);
            }
            else
            {
                return string.Format("{0} | {1} | {2} | {3} | {4} | {5}",
                    stop.EntityKey,
                    stop.StopState_State,
                    stop.IsCancelled,
                    stop.Index,
                    stop.ArrivalTime != null ? ToString(stop.ArrivalTime) : string.Empty,
                    stop.DepartureTime != null ? ToString(stop.DepartureTime) : string.Empty);
            }
        }

        public static string ToString(StopAction stopAction)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6}",
                stopAction.EntityKey,
                stopAction.OrderEntityKey,
                stopAction.OrderIdentifier,
                stopAction.OrderState_OrderState,
                stopAction.OrderType_OrderType,
                stopAction.StopActionType_Type,
                stopAction.StopActionLineItemQuantities != null ? string.Join(" | ", stopAction.StopActionLineItemQuantities.Select(stopActionLineItemQuantities => ToString(stopActionLineItemQuantities))) : string.Empty);
        }

        public static string ToString(StopActionLineItemQuantities stopActionLineItemQuantities)
        {
            return string.Format("{0} | {1}",
                stopActionLineItemQuantities.EntityKey,
                stopActionLineItemQuantities.LineItem != null ? ToString(stopActionLineItemQuantities.LineItem) : string.Empty);
        }

        public static string ToString(Stop.Identity stopIdentity)
        {
            return string.Format("{0} | {1}",
                stopIdentity.StopEntityKey.HasValue ? stopIdentity.StopEntityKey.Value.ToString() : string.Empty,
                stopIdentity.DeviceStopId.HasValue ? stopIdentity.DeviceStopId.Value.ToString() : string.Empty);
        }

        public static string ToString(StopSequenceNotification stopSequenceNotification)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5}",
                stopSequenceNotification.EntityKey,
                stopSequenceNotification.NotificationType_NotificationType,
                stopSequenceNotification.RecipientEntityKey,
                stopSequenceNotification.RouteEntityKey.HasValue ? stopSequenceNotification.RouteEntityKey.Value.ToString() : string.Empty,
                stopSequenceNotification.TelematicsDeviceEntityKey.HasValue ? stopSequenceNotification.TelematicsDeviceEntityKey.Value.ToString() : string.Empty,
                stopSequenceNotification.Stops != null ? string.Join(" | ", stopSequenceNotification.Stops.Select(stop => ToString(stop))) : string.Empty);
        }

        public static string ToString(StopStateChangeNotification stopStateChangeNotification)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7}",
                stopStateChangeNotification.EntityKey,
                stopStateChangeNotification.NotificationType_NotificationType,
                stopStateChangeNotification.RecipientEntityKey,
                stopStateChangeNotification.RouteEntityKey.HasValue ? stopStateChangeNotification.RouteEntityKey.Value.ToString() : string.Empty,
                stopStateChangeNotification.TelematicsDeviceEntityKey.HasValue ? stopStateChangeNotification.TelematicsDeviceEntityKey.Value.ToString() : string.Empty,
                stopStateChangeNotification.StopEntityKey,
                stopStateChangeNotification.StopState_State,
                stopStateChangeNotification.IsCancelled);
        }

        public static string ToString(TaskOpenCloseOverrideDetail taskOpenCloseOverrideDetail)
        {
            return string.Format("{0} | {1}",
                taskOpenCloseOverrideDetail.EntityKey,
                taskOpenCloseOverrideDetail.DailyTimePeriod != null ? ToString(taskOpenCloseOverrideDetail.DailyTimePeriod) : string.Empty);
        }

        public static string ToString(TaskServiceWindowOverrideDetail taskServiceWindowOverrideDetail)
        {
            return string.Format("{0} | {1} | {2}",
                taskServiceWindowOverrideDetail.EntityKey,
                taskServiceWindowOverrideDetail.DailyTimePeriod != null ? ToString(taskServiceWindowOverrideDetail.DailyTimePeriod) : string.Empty,
                taskServiceWindowOverrideDetail.RequiresMultipleWorkers);
        }

        public static string ToString(TelematicsDevice telematicsDevice)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8}",
                telematicsDevice.EntityKey,
                telematicsDevice.Identifier,
                telematicsDevice.BusinessUnitEntityKey,
                telematicsDevice.RegionEntityKeys != null ? string.Join(" | ", telematicsDevice.RegionEntityKeys) : string.Empty,
                telematicsDevice.Active,
                telematicsDevice.DevicePhoneNumber,
                telematicsDevice.EquipmentEntityKey.HasValue ? telematicsDevice.EquipmentEntityKey.Value.ToString() : string.Empty,
                telematicsDevice.TelematicsProviderType_ProviderType,
                telematicsDevice.VisibleInAllRegions);
        }

        public static string ToString(TransferErrorCode transferErrorCode)
        {
            return transferErrorCode.ErrorCode_Status;
        }

        public static string ToString(TransferTaskSpec transferTaskSpec)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8}",
                transferTaskSpec.PickupLocationEntityKey,
                transferTaskSpec.DeliveryLocationEntityKey,
                transferTaskSpec.RequiredOriginEntityKey,
                transferTaskSpec.RequiredDestinationEntityKey,
                transferTaskSpec.Quantities != null ? ToString(transferTaskSpec.Quantities) : string.Empty,
                transferTaskSpec.PickupOpenCloseOverrides != null ? string.Join(" | ", transferTaskSpec.PickupOpenCloseOverrides.Select(openCloseOverride => ToString(openCloseOverride))) : string.Empty,
                transferTaskSpec.DeliveryOpenCloseOverrides != null ? string.Join(" | ", transferTaskSpec.DeliveryOpenCloseOverrides.Select(openCloseOverride => ToString(openCloseOverride))) : string.Empty,
                transferTaskSpec.PickupServiceWindowOverrides != null ? string.Join(" | ", transferTaskSpec.PickupServiceWindowOverrides.Select(serviceWindowOverride => ToString(serviceWindowOverride))) : string.Empty,
                transferTaskSpec.DeliveryServiceWindowOverrides != null ? string.Join(" | ", transferTaskSpec.DeliveryServiceWindowOverrides.Select(serviceWindowOverride => ToString(serviceWindowOverride))) : string.Empty);
        }

        public static string ToString(UnassignedOrderGroup unassignedOrderGroup)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5}",
                unassignedOrderGroup.RegionEntityKey,
                unassignedOrderGroup.SessionEntityKey,
                unassignedOrderGroup.SessionDate,
                unassignedOrderGroup.SessionDescription,
                unassignedOrderGroup.SessionMode_SessionMode,
                unassignedOrderGroup.EntityKey,
                unassignedOrderGroup.LocationEntityKey,
                unassignedOrderGroup.LocationIdentifier,
                unassignedOrderGroup.OrderEntityKeys != null ? string.Join(" | ", unassignedOrderGroup.OrderEntityKeys) : string.Empty,
                unassignedOrderGroup.OrderIdentifiers != null ? string.Join(" | ", unassignedOrderGroup.OrderIdentifiers) : string.Empty,
                unassignedOrderGroup.BeginDate,
                unassignedOrderGroup.EndDate);
        }

        public static string ToString(ValidationFailure validationFailure)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5}",
                validationFailure.FailureType_Type,
                validationFailure.Property,
                validationFailure.Value,
                validationFailure.Minimum,
                validationFailure.Maximum,
                validationFailure.Pattern);
        }

        public static string ToString(Worker worker)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8} | {9}",
                worker.EntityKey,
                worker.Identifier,
                worker.BusinessUnitEntityKey,
                worker.CreatedInRegionEntityKey,
                worker.RegionEntityKeys != null ? string.Join(" | ", worker.RegionEntityKeys) : string.Empty,
                worker.VisibleInAllRegions,
                worker.Name != null ? ToString(worker.Name) : string.Empty,
                worker.WorkerTypeEntityKey,
                worker.DepotEntityKey.HasValue ? worker.DepotEntityKey.Value.ToString() : string.Empty,
                worker.EquipmentEntityKey.HasValue ? worker.EquipmentEntityKey.Value.ToString() : string.Empty);
        }

        public static string ToString(WorkerType workerType)
        {
            return string.Format("{0} | {1} | {2} | {3} | {4} | {5}",
                workerType.EntityKey,
                workerType.Identifier,
                workerType.BusinessUnitEntityKey,
                workerType.CreatedInRegionEntityKey,
                workerType.RegionEntityKeys != null ? string.Join(" | ", workerType.RegionEntityKeys) : string.Empty,
                workerType.VisibleInAllRegions);
        }

        #endregion

        #region Public Methods

        public ApexConsumer(
            Region region,
            log4net.ILog logger)
        {
            _BusinessUnitEntityKey = region.BusinessUnitEntityKey;
            _Region = region;
            _RegionContext = new SingleRegionContext
            {
                BusinessUnitEntityKey = region.BusinessUnitEntityKey,
                RegionEntityKey = region.EntityKey
            };
            _Logger = logger;
            _QueryServiceClient = new QueryServiceClient("BasicHttpBinding_IQueryService", MainService.QueryServiceUrl);
            _MappingServiceClient = new MappingServiceClient("BasicHttpBinding_IMappingService", MainService.RegionUrlSets[region.EntityKey].MappingService);
            _RoutingServiceClient = new RoutingServiceClient("BasicHttpBinding_IRoutingService", MainService.RegionUrlSets[region.EntityKey].RoutingService);
        }

        public ApexConsumer(
            long businessUnitEntityKey,
            log4net.ILog logger)
        {
            _BusinessUnitEntityKey = businessUnitEntityKey;
            _RegionContext = new MultipleRegionContext
            {
                BusinessUnitEntityKey = businessUnitEntityKey,
                Mode = MultipleRegionMode.All
            };
            _Logger = logger;
            _QueryServiceClient = new QueryServiceClient("BasicHttpBinding_IQueryService", MainService.QueryServiceUrl);
            _RoutingServiceClient = new RoutingServiceClient("BasicHttpBinding_IRoutingService", MainService.DefaultRoutingServiceUrl);
        }

        public SaveResult[] DeleteNotifications(
            out ErrorLevel errorLevel,
            out string fatalErrorMessage,
            Notification[] notifications)
        {
            SaveResult[] saveResults = null;
            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            try
            {
                saveResults = _RoutingServiceClient.Save(
                    MainService.SessionHeader,
                    null,
                    notifications.Select(notification => new Notification
                    {
                        Action = ActionType.Delete,
                        EntityKey = notification.EntityKey,
                        Version = notification.Version
                    }).ToArray(),
                    new SaveOptions
                    {
                        InclusionMode = PropertyInclusionMode.All
                    });
                if (saveResults == null)
                {
                    _Logger.Error("DeleteNotifications | " + string.Join(" | ", notifications.Select(notification => ToString(notification))) + " | Failed with a null result.");
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    for (int i = 0; i < saveResults.Length; i++)
                    {
                        if (saveResults[i].Error != null)
                        {
                            _Logger.Error("DeleteNotifications | " + ToString(notifications[i]) + " | Failed with Error: " + ToString(saveResults[i].Error));
                            errorLevel = ErrorLevel.Partial;
                        }
                    }
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("DeleteNotifications | " + string.Join(" | ", notifications.Select(notification => ToString(notification))) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("DeleteNotifications | " + string.Join(" | ", notifications.Select(notification => ToString(notification))), ex);
                errorLevel = ErrorLevel.Transient;
            }
            return saveResults;
        }

        public GeocodeResult[] Geocode(
            out ErrorLevel errorLevel,
            out string fatalErrorMessage,
            out bool timeout,
            Address[] addresses)
        {
            GeocodeResult[] geocodeResults = null;
            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            timeout = false;
            try
            {
                geocodeResults = _MappingServiceClient.Geocode(
                    MainService.SessionHeader,
                    _RegionContext,
                    new GeocodeCriteria
                    {
                        NamedPlaces = addresses.Select(address => new NamedPlace { PlaceAddress = address }).ToArray()
                    },
                    new GeocodeOptions
                    {
                        NetworkArcCandidatePropertyInclusionMode = PropertyInclusionMode.All,
                        NetworkPOICandidatePropertyInclusionMode = PropertyInclusionMode.All,
                        NetworkPointAddressCandidatePropertyInclusionMode = PropertyInclusionMode.All
                    });
                if (geocodeResults == null)
                {
                    _Logger.Error("Geocode | " + string.Join(" | ", addresses.Select(address => ToString(address))) + " | Failed with a null result.");
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    for (int i = 0; i < geocodeResults.Length; i++)
                    {
                        if (geocodeResults[i] == null || geocodeResults[i].Results == null || geocodeResults[i].Results.Length == 0)
                        {
                            _Logger.Error("Geocode | " + ToString(addresses[i]) + " | Failed with a null result.");
                            errorLevel = ErrorLevel.Partial;
                        }
                    }
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("Geocode | " + string.Join(" | ", addresses.Select(address => ToString(address))) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("Geocode | " + string.Join(" | ", addresses.Select(address => ToString(address))), ex);
                errorLevel = ErrorLevel.Transient;
                timeout = ex is TimeoutException || ex is CommunicationException;
            }
            return geocodeResults;
        }

        public DailyRoutingSession[] RetrieveDailyRoutingSessions(
            out ErrorLevel errorLevel,
            out string fatalErrorMessage,
            DateTime[] startDates)
        {
            DailyRoutingSession[] dailyRoutingSessions = null;
            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            try
            {
                RetrievalResults retrievalResults = _QueryServiceClient.Retrieve(
                    MainService.SessionHeader,
                    _RegionContext,
                    new RetrievalOptions
                    {
                        Expression = new AndExpression
                        {
                            Expressions = new SimpleExpressionBase[]
                            {
                                new EqualToExpression
                                {
                                    Left = new PropertyExpression { Name = "Description" },
                                    Right = new ValueExpression { Value = DEFAULT_IDENTIFIER }
                                },
                                new InExpression
                                {
                                    Left = new PropertyExpression { Name = "StartDate" },
                                    Right = new ValueExpression { Value = startDates.Select(startDate => startDate.ToString(DATE_FORMAT)).ToArray() }
                                }
                            }
                        },
                        //TODO
                        PropertyInclusionMode = PropertyInclusionMode.AccordingToPropertyOptions,
                        PropertyOptions = new DailyRoutingSessionPropertyOptions
                        {
                            StartDate = true
                        },
                        Type = Enum.GetName(typeof(RetrieveType), RetrieveType.DailyRoutingSession)
                    });
                if (retrievalResults.Items == null)
                {
                    _Logger.Error("RetrieveDailyRoutingSessions | " + string.Join(" | ", startDates) + " | Failed with a null result.");
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    dailyRoutingSessions = retrievalResults.Items.Cast<DailyRoutingSession>().ToArray();
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("RetrieveDailyRoutingSessions | " + string.Join(" | ", startDates) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("RetrieveDailyRoutingSessions | " + string.Join(" | ", startDates), ex);
                errorLevel = ErrorLevel.Transient;
            }
            return dailyRoutingSessions;
        }

        public Notification[] RetrieveNotifications(
            out ErrorLevel errorLevel,
            out string fatalErrorMessage,
            long recipientEntityKey)
        {
            Notification[] notifications = null;
            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            try
            {
                RetrievalResults retrievalResults = _QueryServiceClient.Retrieve(
                    MainService.SessionHeader,
                    _RegionContext,
                    new RetrievalOptions
                    {
                        Expression = new EqualToExpression
                        {
                            Left = new PropertyExpression { Name = "RecipientEntityKey" },
                            Right = new ValueExpression { Value = recipientEntityKey }
                        },
                        Paged = true,
                        PageIndex = 0,
                        PageSize = BATCH_SIZE,
                        PropertyInclusionMode = PropertyInclusionMode.All,
                        Type = Enum.GetName(typeof(RetrieveType), RetrieveType.Notification)
                    });
                if (retrievalResults.Items == null)
                {
                    _Logger.Error("RetrieveNotifications | " + recipientEntityKey + " | Failed with a null result.");
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    notifications = retrievalResults.Items.Cast<Notification>().ToArray();
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("RetrieveNotifications | " + recipientEntityKey + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("RetrieveNotifications | " + recipientEntityKey, ex);
                errorLevel = ErrorLevel.Transient;
            }
            return notifications;
        }

        public Route RetrieveRoute(
            out ErrorLevel errorLevel,
            out string fatalErrorMessage,
            long entityKey)
        {
            Route route = null;
            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            try
            {
                RetrievalResults retrievalResults = _QueryServiceClient.Retrieve(
                    MainService.SessionHeader,
                    _RegionContext,
                    new RetrievalOptions
                    {
                        Expression = new EqualToExpression
                        {
                            Left = new PropertyExpression { Name = "EntityKey" },
                            Right = new ValueExpression { Value = entityKey }
                        },
                        //TODO
                        PropertyInclusionMode = PropertyInclusionMode.AccordingToPropertyOptions,
                        PropertyOptions = new RoutePropertyOptions
                        {
                            Identifier = true,
                            RegionEntityKey = true,
                            StartTime = true,
                            Stops = true,
                            StopsOptions = new StopPropertyOptions
                            {
                                Actions = true,
                                ActionsOptions = new StopActionPropertyOptions
                                {
                                    StopActionLineItemQuantities = true,
                                    StopActionLineItemQuantitiesOptions = new StopActionLineItemQuantitiesPropertyOptions
                                    {
                                        LineItem = true,
                                        LineItemOptions = new LineItemPropertyOptions
                                        {
                                            CustomProperties = true,
                                            Identifier = true,
                                            LineItemType_Type = true,
                                            PlannedQuantities = true,
                                            Quantities = true
                                        }
                                    }
                                },
                                ArrivalTime = true,
                                DepartureTime = true,
                                IsCancelled = true
                            }
                        },
                        Type = Enum.GetName(typeof(RetrieveType), RetrieveType.Route)
                    });
                if (retrievalResults.Items == null)
                {
                    _Logger.Error("RetrieveRoute | " + entityKey + " | Failed with a null result.");
                    errorLevel = ErrorLevel.Transient;
                }
                else if (retrievalResults.Items.Length == 0)
                {
                    fatalErrorMessage = "Route does not exist.";
                    _Logger.Error("RetrieveRoute | " + entityKey + " | " + fatalErrorMessage);
                    errorLevel = ErrorLevel.Fatal;
                }
                else
                {
                    route = (Route)retrievalResults.Items[0];
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("RetrieveRoute | " + entityKey + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("RetrieveRoute | " + entityKey, ex);
                errorLevel = ErrorLevel.Transient;
            }
            return route;
        }

        public List<ServiceLocation> RetrieveServiceLocationsFromStagingTable (string regionId, string staged, out bool errorRetrieveSLFromStagingTable, out string errorRetrieveSLFromStagingTableMessage)
        { 
        
            errorRetrieveSLFromStagingTable = false;
            errorRetrieveSLFromStagingTableMessage = string.Empty;
            List<ServiceLocation> retrieveList = new List<ServiceLocation>();
            DBAccess.IntegrationDBAccessor DBAccessor = new DBAccess.IntegrationDBAccessor(_Logger);

            
            try
            {

                retrieveList = DBAccessor.SelectStagedServiceLocations(regionId, staged).Cast<ServiceLocation>().ToList();

                if (retrieveList == null)
                {
                    errorRetrieveSLFromStagingTable = true;
                    _Logger.ErrorFormat(errorRetrieveSLFromStagingTableMessage);
                    return null;
                    
                }
                else if (retrieveList.Count == 0)
                {
                    errorRetrieveSLFromStagingTable = true;
                    errorRetrieveSLFromStagingTableMessage = String.Format("No New Staged Service Locations found in Staged Service Locatoins Table for {0}", regionId);
                    _Logger.ErrorFormat(errorRetrieveSLFromStagingTableMessage);
                    return retrieveList;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error(ex.Message);
            }

            return null;
        }

        public List<Order> RetrieveOrderesFromStagingTable(string regionId, string staged, out bool errorRetrieveOrdersFromStagingTable, out string errorRetrieveOrdersFromStagingTableMessage)
        {

            errorRetrieveOrdersFromStagingTable = false;
            errorRetrieveOrdersFromStagingTableMessage = string.Empty;
            List<Order> retrieveListOrders = new List<Order>();
            DBAccess.IntegrationDBAccessor DBAccessor = new DBAccess.IntegrationDBAccessor(_Logger);


            try
            {

                retrieveListOrders = DBAccessor.SelectStagedOrders(regionId, staged).Cast<Order>().ToList();

                if (retrieveListOrders == null)
                {
                    errorRetrieveOrdersFromStagingTable = true;
                    _Logger.ErrorFormat(errorRetrieveOrdersFromStagingTableMessage);
                    return null;

                }
                else if (retrieveListOrders.Count == 0)
                {
                    errorRetrieveOrdersFromStagingTable = true;
                    errorRetrieveOrdersFromStagingTableMessage = String.Format("No New Staged Orders found in Staged Order Table for {0}", regionId);
                    _Logger.ErrorFormat(errorRetrieveOrdersFromStagingTableMessage);
                    return retrieveListOrders;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error(ex.Message);
            }

            return null;
        }

        public List<Order> RetrieveDummyOrdersFromCSV (string regionId, string staged, out bool errorRetrieveDummyOrdersFromCSV, out string errorRetrieveDummyOrdersFromCSVMessage)
        {

            errorRetrieveDummyOrdersFromCSV = false;
            errorRetrieveDummyOrdersFromCSVMessage = string.Empty;
            List<Order> retrieveListDummyOrders = new List<Order>();

            //read csv file
            var lines = File.ReadLines(Config.DummyOrderCSVFile).Select(a => a.Split(','));
            int linelength = lines.First().Count();
            var CSVFile = lines.Skip(1)
                .SelectMany(x => x)
                .Select((v, i) => new { Value = v, Index = i % linelength })
                .Where(x => x.Index == 2 || x.Index == 3)
                .Select(x => x.Value);

                try
            {

                retrieveListDummyOrders = 

                if (retrieveListDummyOrders == null)
                {
                    errorRetrieveDummyOrdersFromCSV = true;
                    _Logger.ErrorFormat(errorRetrieveDummyOrdersFromCSVMessage);
                    return null;

                }
                else if (retrieveListDummyOrders.Count == 0)
                {
                    errorRetrieveDummyOrdersFromCSV = true;
                    errorRetrieveDummyOrdersFromCSVMessage = String.Format("No Dummy Orders found in the CSV file {0} for region {1}", regionId);
                    _Logger.ErrorFormat(errorRetrieveDummyOrdersFromCSVMessage);
                    return retrieveListDummyOrders;
                }

                return retrieveListDummyOrders;
            }
            catch (Exception ex)
            {
                _Logger.Error(ex.Message);
            }

            return null;
        }

        public ServiceLocation[] RetrieveServiceLocations(
            out ErrorLevel errorLevel,
            out string fatalErrorMessage,
            string[] identifiers,
            bool retrieveIdentifierOnly = false)
        {
            ServiceLocation[] serviceLocations = null;
            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            try
            {
                RetrievalResults retrievalResults = _QueryServiceClient.Retrieve(
                    MainService.SessionHeader,
                    _RegionContext,
                    new RetrievalOptions
                    {
                        Expression = new InExpression
                        {
                            Left = new PropertyExpression { Name = "Identifier" },
                            Right = new ValueExpression { Value = identifiers }
                        },
                        //TODO
                        PropertyInclusionMode = retrieveIdentifierOnly ? PropertyInclusionMode.AccordingToPropertyOptions : PropertyInclusionMode.All,
                        PropertyOptions = new ServiceLocationPropertyOptions
                        {
                            Identifier = true
                        },
                        Type = Enum.GetName(typeof(RetrieveType), RetrieveType.ServiceLocation)
                    });
                if (retrievalResults.Items == null)
                {
                    _Logger.Error("RetrieveServiceLocations | " + string.Join(" | ", identifiers) + " | Failed with a null result.");
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    serviceLocations = retrievalResults.Items.Cast<ServiceLocation>().ToArray();
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("RetrieveServiceLocations | " + string.Join(" | ", identifiers) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("RetrieveServiceLocations | " + string.Join(" | ", identifiers), ex);
                errorLevel = ErrorLevel.Transient;
            }
            return serviceLocations;
        }

        public SaveResult[] SaveDailyRoutingSessions(
            out ErrorLevel errorLevel,
            out string fatalErrorMessage,
            DateTime[] startDates)
        {
            SaveResult[] saveResults = null;
            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            try
            {
                saveResults = _RoutingServiceClient.Save(
                    MainService.SessionHeader,
                    _RegionContext,
                    startDates.Select(startDate => new DailyRoutingSession
                    {
                        Action = ActionType.Add,
                        Description = DEFAULT_IDENTIFIER,
                        NumberOfTimeUnits = 1,
                        RegionEntityKey = _Region.EntityKey,
                        SessionMode_Mode = Enum.GetName(typeof(Apex.SessionMode), Apex.SessionMode.Operational),
                        StartDate = startDate.ToString(DATE_FORMAT),
                        TimeUnit_TimeUnitType = Enum.GetName(typeof(TimeUnit), TimeUnit.Day)
                    }).ToArray(),
                    new SaveOptions
                    {
                        //TODO
                        InclusionMode = PropertyInclusionMode.All,
                        ReturnInclusionMode = PropertyInclusionMode.AccordingToPropertyOptions,
                        ReturnPropertyOptions = new DailyRoutingSessionPropertyOptions
                        {
                            StartDate = true
                        },
                        ReturnSavedItems = true
                    });
                if (saveResults == null)
                {
                    _Logger.Error("SaveDailyRoutingSessions | " + string.Join(" | ", startDates) + " | Failed with a null result.");
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    for (int i = 0; i < saveResults.Length; i++)
                    {
                        if (saveResults[i].Error != null)
                        {
                            _Logger.Error("SaveDailyRoutingSessions | " + startDates[i] + " | Failed with Error: " + ToString(saveResults[i].Error));
                            errorLevel = ErrorLevel.Partial;
                        }
                    }
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("SaveDailyRoutingSessions | " + string.Join(" | ", startDates) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("SaveDailyRoutingSessions | " + string.Join(" | ", startDates), ex);
                errorLevel = ErrorLevel.Transient;
            }
            return saveResults;
        }

        public SaveResult[] SaveOrders(
            out ErrorLevel errorLevel,
            out string fatalErrorMessage,
            OrderSpec[] orderSpecs)
        {
            SaveResult[] saveResults = null;
            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            try
            {
                saveResults = _RoutingServiceClient.SaveOrders(
                    MainService.SessionHeader,
                    _RegionContext,
                    orderSpecs,
                    new SaveOptions
                    {
                        //TODO
                        InclusionMode = PropertyInclusionMode.All
                    });
                if (saveResults == null)
                {
                    _Logger.Error("SaveOrders | " + string.Join(" | ", orderSpecs.Select(order => ToString(order))) + " | Failed with a null result.");
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    for (int i = 0; i < saveResults.Length; i++)
                    {
                        if (saveResults[i].Error != null)
                        {
                            _Logger.Error("SaveOrders | " + ToString(orderSpecs[i]) + " | Failed with Error: " + ToString(saveResults[i].Error));
                            errorLevel = ErrorLevel.Partial;
                        }
                    }
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("SaveOrders | " + string.Join(" | ", orderSpecs.Select(order => ToString(order))) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("SaveOrders | " + string.Join(" | ", orderSpecs.Select(order => ToString(order))), ex);
                errorLevel = ErrorLevel.Transient;
            }
            return saveResults;
        }

        
        public SaveResult[] SaveServiceLocations(
            out ErrorLevel errorLevel,
            out string fatalErrorMessage,
            ServiceLocation[] serviceLocations)
        {
            SaveResult[] saveResults = null;
            errorLevel = ErrorLevel.None;
            fatalErrorMessage = string.Empty;
            try
            {
                saveResults = _RoutingServiceClient.Save(
                    MainService.SessionHeader,
                    _RegionContext,
                    serviceLocations,
                    new SaveOptions
                    {
                        //TODO
                        InclusionMode = PropertyInclusionMode.AccordingToPropertyOptions,
                        PropertyOptions = new ServiceLocationPropertyOptions
                        {
                            Address = true,
                            BusinessUnitEntityKey = true,
                            Coordinate = true,
                            DayOfWeekFlags_DeliveryDays = true,
                            Description = true,
                            GeocodeAccuracy_GeocodeAccuracy = true,
                            GeocodeMethod_GeocodeMethod = true,
                            Identifier = true,
                            PhoneNumber = true,
                            RegionEntityKeys = true,
                            ServiceTimeTypeEntityKey = true,
                            StandardInstructions = true,
                            StandingDeliveryQuantities = true,
                            StandingPickupQuantities = true,
                            TimeWindowTypeEntityKey = true,
                            VisibleInAllRegions = true,
                            WorldTimeZone_TimeZone = true
                        }
                    });
                if (saveResults == null)
                {
                    _Logger.Error("SaveServiceLocations | " + string.Join(" | ", serviceLocations.Select(serviceLocation => ToString(serviceLocation))) + " | Failed with a null result.");
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    for (int i = 0; i < saveResults.Length; i++)
                    {
                        if (saveResults[i].Error != null)
                        {
                            _Logger.Error("SaveServiceLocations | " + ToString(serviceLocations[i]) + " | Failed with Error: " + ToString(saveResults[i].Error));
                            errorLevel = ErrorLevel.Partial;
                        }
                    }
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                string errorMessage = "TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message;
                _Logger.Error("SaveServiceLocations | " + string.Join(" | ", serviceLocations.Select(serviceLocation => ToString(serviceLocation))) + " | " + errorMessage);
                if (tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.SessionAuthenticationFailed) || tec.Detail.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.InvalidEndpointRequest))
                {
                    _Logger.Info("Session has expired. New session required.");
                    MainService.SessionRequired = true;
                    errorLevel = ErrorLevel.Transient;
                }
                else
                {
                    errorLevel = ErrorLevel.Fatal;
                    fatalErrorMessage = errorMessage;
                }
            }
            catch (Exception ex)
            {
                _Logger.Error("SaveServiceLocations | " + string.Join(" | ", serviceLocations.Select(serviceLocation => ToString(serviceLocation))), ex);
                errorLevel = ErrorLevel.Transient;
            }
            return saveResults;
        }

        #endregion

    }
}
