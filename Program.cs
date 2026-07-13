using System.Net;
using System.Linq;
using System.Net.Sockets;
using Shared.Source.tools;
using AVcontrol;
using System.Text;
using System.Reflection.Metadata;

namespace MyProgram
{

    internal class Program
    {
        private class CommandOptions
        {
            public bool NoLog { get; set; }
            public bool CheckNew { get; set; }
            public bool Monitoring { get; set; }
        }

        private static async Task Main(string[] args)
        {
            var options = ParseArguments(args);

            if (!options.NoLog)
            {
                Logger.Init(Config.appLogPath);
                Logger.Log($"Application started in {DateTime.Now}");
            }

            await using var deviceDb = new DatabaseController<MonitoredDevice>(Config.addressesDataBasePath);
            await using var minuteController = new MinuteTrafficStore(Config.todayStatisticDataBasePath);

            await StartMonitoring(options, deviceDb, minuteController);

            // var monitor = new Monitor();
            // Console.WriteLine(await monitor.GetSnmpValueAsync("192.168.11.89", ["1.3.6.1.2.1.2.2.1.10.1"])); есть!
            // var md = new MonitoredDevice();
            // md.ID = "192.168.11.63";
            // md.TimeOut = 500;
            // md.OpenInterfaces = [1];

            // for (int i = 0; i < 4; i++)
            // {
            //     Console.WriteLine("===");
            //     await TrafficProcessor.UpdateDeviceStatisticsAsync(md, monitor, minuteController);
            //     await Task.Delay(20 * 1000);
            // } тоже работает в здоровом формате
        }
        private static async Task StartMonitoring(CommandOptions options, IDatabaseController<MonitoredDevice> deviceDb, MinuteTrafficStore minuteController)
        {
            var monitor = new Monitor();
            if (options.CheckNew) await monitor.ConfigureIpListAsync(deviceDb);

            if (!options.Monitoring) return;

            var allDevices = await deviceDb.GetAllAsync();
            int batchCount = Math.Min(allDevices.Count, 40);
            Logger.Log($"dev count: {allDevices.Count} -- batch count : {batchCount}");
            var deviceBatches = ListExtensions.SplitIntoBalancedGroups(allDevices, batchCount);

            foreach (var group in deviceBatches)
            {
                Logger.Log("Group:");
                foreach (var dev in group)
                {
                    var sb = new StringBuilder();
                    foreach (var intrf in dev.OpenInterfaces) sb.Append($", {intrf}");
                    Logger.Log($"\t{dev.ID} : {dev.TimeOut} microsec{sb}");
                }
            }

            var tasks = new List<Task>();
            int startDelay = 60 * 1000 / deviceBatches.Count;
            var cts = new CancellationTokenSource();

            if (!options.NoLog) Logger.Log($"Created {deviceBatches.Count} batches, start delay {startDelay}ms");

            Task savingTask = TrafficProcessor.SaveDayStatistic(minuteController, deviceDb, cts.Token);

            int batchIndex = 0;
            foreach (var batch in deviceBatches)
            {
                int currentBatch = batchIndex;
                tasks.Add(Task.Run(async () =>
                {
                    while (!cts.IsCancellationRequested)
                    {
                        try
                        {
                            foreach (var device in batch)
                            {
                                await TrafficProcessor.UpdateDeviceStatisticsAsync(device, monitor, minuteController);
                            }
                            await Task.Delay(20 * 1000, cts.Token);
                        }
                        catch (Exception e)
                        {
                            if (!options.NoLog) Logger.Error($"Batch {currentBatch + 1} error: {e}");
                        }
                    }
                }, cts.Token));

                await Task.Delay(startDelay);
                batchIndex++;
            }

            if (!options.NoLog) Logger.Log("All batches started");
            Console.WriteLine($"End setup at {DateTime.Now:HH:mm:ss}");

            Console.Write("Press any key: ");
            Console.ReadKey();

            if (!options.NoLog) Logger.Log("Stop signal received, cancelling tasks");
            cts.Cancel();
            foreach (var t in tasks) await t;
            await savingTask;
            cts.Dispose();

            if (!options.NoLog)
            {
                Logger.Log("Application finished");
                await Logger.DisposeAsync();
            }
        }

        private static CommandOptions ParseArguments(string[] args)
        {
            var options = new CommandOptions
            {
                NoLog = args.Contains("--no-log") || args.Contains("-n"),
                CheckNew = args.Contains("--check") || args.Contains("-c"),
                Monitoring = args.Contains("--monitor") || args.Contains("-m")
            };

            return options;
        }

        private static string GetArgValue(string[] args, string shortName, string longName = null)
        {
            for (int i = 0; i < args.Length; i++)
            {
                string arg = args[i];
                if (longName != null && arg.StartsWith(longName + "="))
                    return arg.Substring(longName.Length + 1);
                if ((longName != null && arg == longName) || arg == shortName)
                {
                    if (i + 1 < args.Length && !args[i + 1].StartsWith("-"))
                        return args[i + 1];
                }
            }
            return null;
        }
        private static string GetArgValue(string[] args, string longName) => GetArgValue(args, null, longName);
}

    public class MonitoredDevice : IHasId
    {
        public string ID { get; set; }
        public bool IsDeleted { get; set; }
        public int TimeOut { get; set; }
        public UInt16[] OpenInterfaces { get; set; } = Array.Empty<UInt16>();
    }

    public class GlobalDeviceTrafic : IHasId
    {
        public string ID { get; set; }
        public bool IsDeleted { get; set; }
        public Dictionary<string, (UInt64 In, UInt64 Out)[]> globalState { get; set; }
    }
}