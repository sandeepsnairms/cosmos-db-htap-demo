using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Bogus;
using Microsoft.Extensions.Configuration;
using Microsoft.Azure.Cosmos;
using System.Net;
using System.ComponentModel;
using System.Threading;
using System.Collections.Concurrent;
using System.Diagnostics;
using Database = Microsoft.Azure.Cosmos.Database;
using Container = Microsoft.Azure.Cosmos.Container;

namespace CallLog
{
    public record Device(
        string OS,
        string Make        
        );


    public record Call(
         string id,
            DateTime StartDateTime,
            DateTime EndDateTime,
            int DurationSec,
            string CallFrom,
            string CallTo,
            string CallType, //local,national,international
            int CallLocationId, //LocationId of current region
            int BaseLocationId, //Base location of the customer
            bool IsRoaming,
            bool IsIncoming,
            string SubscriberId,
            string BillCycle,
            string pk, // Synthetic  keywith format subscriberId.BillCycle           
            Device device
        );

    

    internal class CallLogger
    {
        List<Call> CallDocs;

        IConfigurationRoot config;

        Faker f = new Faker();
        Device currDevice;

        enum CallTypes
        {
            local,
            national            
        }


        internal CallLogger()
        {
            CallDocs = new List<Call>();

            var configuration = new ConfigurationBuilder()
                                     .SetBasePath(Directory.GetCurrentDirectory())
                                     .AddJsonFile($"appsettings.json");

            config = configuration.Build();

            //Device Info
            
            Device dv1 = new Device("iOS 15", "iPhone 12");
            Device dv2 = new Device("Android 10", "OnePlus 7");
            if (f.Random.Bool())
                currDevice = dv2;
            else
                currDevice = dv1;
        }

        private static CosmosClient GetBulkClientInstance(
            string endpoint,
            string authKey)
        {
            // </Initialization>
            return new CosmosClient(endpoint, authKey, new CosmosClientOptions() { AllowBulkExecution = true });
        }

        internal async Task InsertCalls()
        {
            var endpointUrl = config["endpoint"];
            var authKey = config["authKey"];
            var databaseName = config["databaseName"];
            var containerName = config["containerName"];


            CosmosClient client = GetBulkClientInstance(endpointUrl, authKey);
            Database database = client.GetDatabase(databaseName);
            Container container = database.GetContainer(containerName); 

            await CreateItemsConcurrentlyAsync(container);

        }

      

        private async Task CreateItemsConcurrentlyAsync(Container container)
        {

            List <Task> tasks = new List<Task>();
            foreach (Call call in CallDocs)
            {

                try
                {

                    tasks.Add(container.CreateItemAsync(call, new PartitionKey(call.pk))
                        .ContinueWith(itemResponse =>
                        {
                            if (!itemResponse.IsCompletedSuccessfully)
                            {
                                AggregateException innerExceptions = itemResponse.Exception.Flatten();
                                if (innerExceptions.InnerExceptions.FirstOrDefault(innerEx => innerEx is CosmosException) is CosmosException cosmosException)
                                {
                                    Console.WriteLine($"Received {cosmosException.StatusCode} ({cosmosException.Message}).");
                                }
                                else
                                {
                                    Console.WriteLine($"Exception {innerExceptions.InnerExceptions.FirstOrDefault()}.");
                                }
                            }
                        }));
                }
                catch(CosmosException e)
                {
                    Console.WriteLine(e.ToString());
                }
            }

            // Wait until all are done
            await Task.WhenAll(tasks);

        }
        internal void GenerateFakeCall(int offset, string subscriberId)
        {
            string id= Guid.NewGuid().ToString();

            if (subscriberId == String.Empty)
                subscriberId = f.Phone.PhoneNumber("091-###-###-####");


            //fake call time and duration
            int duration = f.Random.Number(5, 500);
            DateTime dtStart = GenRandomTime(offset);
            DateTime dtEnd = dtStart.AddSeconds(duration);


            //fake call location and calculate roaming
            int baselocationId = 5;
            int curentlocationId = f.Random.Int(1, 5);
            bool roaming = false;
            if (baselocationId != curentlocationId)
                roaming = true;

            //fake phone numbers
            string callFrom = String.Empty;
            string callTo = String.Empty;
            string callType = String.Empty;
            string phoneNumber = String.Empty;

            bool international = f.Random.Bool();
            if (!international)
            {
                phoneNumber = f.Phone.PhoneNumber("091-###-###-####");
                callType = f.PickRandom<CallTypes>().ToString();
            }
            else
            {
                phoneNumber = f.Phone.PhoneNumber("0##-###-###-####");
                callType = "international";
            }

            //Set incoming/outgoing call
            bool incomingCall = f.Random.Bool();
            if (incomingCall == true)
            {
                callFrom = phoneNumber;
                callTo = subscriberId;
            }
            else
            {
                callTo = phoneNumber;
                callFrom = subscriberId;
            }

            //calc Billing Cycle
            string billCycle = $"{dtStart.ToString("MMM").ToUpper()}{dtStart.Year}";

            //calc partition key
            string pk = $"{subscriberId}.{billCycle}";



            //initialize Record
            var callrecord = new Call(id,dtStart, dtEnd, duration, callFrom, callTo, callType, curentlocationId, baselocationId, roaming, incomingCall, subscriberId, billCycle, pk, currDevice);
             
            CallDocs.Add(callrecord);
  
        }

        private DateTime GenRandomTime(int offset)
        {
            Faker f = new Faker();
            int days = 0;
            int hours = f.Random.Number(0, 23);
            int minutes = f.Random.Number(0, 59);
            int seconds = f.Random.Number(0, 59);


            if(offset>0)
             days = f.Random.Number(offset *-1, 0);                            

            return System.DateTime.Today.AddDays(days).AddHours(hours).AddMinutes(minutes).AddSeconds(seconds);
        }
    }
}

  
