using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BatchCSharpCheatSheet
{
    class Program
    {
        private const string CheatSheetPoolId = "cheat-and-indeed-sheet";

        static void Main(string[] args)
        {
            var batchCredentials = new BatchSharedKeyCredentials(
                Environment.GetEnvironmentVariable("AZURE_BATCH_ENDPOINT"),
                Environment.GetEnvironmentVariable("AZURE_BATCH_ACCOUNT"),
                Environment.GetEnvironmentVariable("AZURE_BATCH_ACCESS_KEY")
            );

            using (var batchClient = BatchClient.Open(batchCredentials))
            {
                // Create a (persistent) pool
                // (Production scenarios: can use auto pools for 'on demand' per-job capacity.)
                var poolExists = batchClient.PoolOperations.ListPools(
                    new ODATADetailLevel(filterClause: $"id eq '{CheatSheetPoolId}'", selectClause: "id")
                ).Any();

                if (!poolExists)
                {
                    var pool = batchClient.PoolOperations.CreatePool(
                        poolId: CheatSheetPoolId,
                        virtualMachineSize: "small",
                        cloudServiceConfiguration: new CloudServiceConfiguration(osFamily: "4"),  // PaaS - use virtualMachineConfiguration overload for IaaS
                        targetDedicated: 1
                    );
                    pool.Commit();
                }

                // Create a job
                var job = batchClient.JobOperations.CreateJob(
                    "CheatSheet_" + DateTime.UtcNow.ToString("HHmmssfff"),
                    new PoolInformation { PoolId = CheatSheetPoolId }
                );
                job.Commit();
                job.Refresh();

                // Add some demanding and completely realistic work to the job
                // (Production scenarios: often done in a job manager task.)
                var task = new CloudTask(
                    id: "just-some-task",
                    commandline: "ping localhost"
                );
                job.AddTask(task);
                task = job.GetTask(task.Id);  // quirks, we has them

                // Wait for the tasks to complete
                var taskStateMonitor = batchClient.Utilities.CreateTaskStateMonitor();
                taskStateMonitor.WaitAll(new[] { task }, TaskState.Completed, TimeSpan.FromMinutes(10));

                // Terminate the job when there's no more work to do
                // (Production scenarios: can use auto termination option.)
                job.Terminate(terminateReason: "PingedAllTheLocalhosts");

                // Get the result
                // (Production scenarios: often persist key outputs in blob storage.)
                var stdoutReference = task.GetNodeFile("stdout.txt");
                var stdout = stdoutReference.ReadAsString();

                Console.WriteLine(stdout);
            }
        }
    }
}
