using System;
using System.Diagnostics;

namespace MyProgram
{
    internal static class TrafficProcessor
    {
        private static uint DeltaFinder(uint curr, uint last)
        {
            return curr >= last ? curr - last : (uint.MaxValue - last) + curr + 1;
        }

        public static async Task UpdateDeviceStatisticsAsync(MonitoredDevice device, Monitor monitor, MinuteTrafficStore snapshotDb)
        {
            DateTime now = DateTime.Now;
            int minuteKey = now.Hour * 60 + now.Minute;

            for (ushort i = 0; i < device.OpenInterfaces.Length; i++)
            {
                var oids = new[]
                {
                    $"1.3.6.1.2.1.2.2.1.10.{device.OpenInterfaces[i]}",
                    $"1.3.6.1.2.1.2.2.1.16.{device.OpenInterfaces[i]}"
                };
                var results = await monitor.GetSnmpValuesAsync(device.ID, oids);

                uint currIn = 0, currOut = 0;
                try
                {
                    currIn = Convert.ToUInt32(results[0]);
                    currOut = Convert.ToUInt32(results[1]);
                }
                catch { /* игнорируем */ }
                
                var lastCounter = snapshotDb.GetLastVal(device.ID, device.OpenInterfaces[i]);

                if (lastCounter.In == 0 && lastCounter.Out == 0)
                {
                    snapshotDb.UpdateLastValue(device.ID, device.OpenInterfaces[i], currIn, currOut);
                    continue;
                }

                uint deltaIn = DeltaFinder(currIn, lastCounter.In);
                uint deltaOut = DeltaFinder(currOut, lastCounter.Out);

                snapshotDb.UpdateLastValue(device.ID, device.OpenInterfaces[i], currIn, currOut);
                snapshotDb.AddTraffic(device.ID, minuteKey, device.OpenInterfaces[i], deltaIn, deltaOut);
            }
        }

        public static async Task SaveDayStatistic(MinuteTrafficStore snapshot, JsonlDatabase<MonitoredDevice> parametrs, CancellationToken ct)
        {
            Logger.Log($"начал ожидать времени записи. сейчас: {DateTime.Now.Hour * 60 + DateTime.Now.Minute} жду: {Config.timeToSave}");
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var now = DateTime.Now;
                    int currentMinute = now.Hour * 60 + now.Minute;
                    if (currentMinute < Config.timeToSave)
                    {
                        await Task.Delay(60 * 5 * 1000, ct);
                        continue;
                    }
                    
                    string todayId = now.ToString("YY:MM:dd");
                    Logger.Log($"{todayId}: НАЧАЛО ЗАПИСИ дневной статистики");
                    await using (var globaleDatabase = new JsonlDatabase<GlobalDeviceTrafic>(Config.globalStatisticDataBasePath))
                    {
                        var existing = await globaleDatabase.GetAsync(todayId, ct);
                        if (existing != null)
                        {
                            var nd = now.Date.AddDays(1);
                            var delay = (int)(nd - now).TotalMilliseconds + 5000;
                            Logger.Log("запись уже существует");
                            await Task.Delay(delay, ct);
                            continue;
                        }
                        Logger.Log("создана новая запись");
                        await snapshot.FinishCurrentMinuteAsync();

                        var records = snapshot.ReadAllRecords().ToList();
                        var aggregated = new Dictionary<string, Dictionary<ushort, (ulong In, ulong Out)>>();

                        foreach (var rec in records)
                        {
                            if (!aggregated.TryGetValue(rec.DeviceId, out var deviceDict))
                            {
                                deviceDict = new Dictionary<ushort, (ulong In, ulong Out)>();
                                aggregated[rec.DeviceId] = deviceDict;
                            }

                            if (deviceDict.TryGetValue(rec.InterfaceIdx, out var current))
                                deviceDict[rec.InterfaceIdx] = (current.In + rec.InBytes, current.Out + rec.OutBytes);
                            else
                                deviceDict[rec.InterfaceIdx] = (rec.InBytes, rec.OutBytes);
                        }

                        var dayStat = new GlobalDeviceTrafic
                        {
                            ID = todayId,
                            IsDeleted = false,
                            globalState = new Dictionary<string, (UInt64 In, UInt64 Out)[]>()
                        };

                        foreach (var kv in aggregated)
                        {
                            var deviceId = kv.Key;
                            var interfaceTraffic = kv.Value;
                            ushort maxIdx = interfaceTraffic.Keys.Max();
                            var arr = new (UInt64 In, UInt64 Out)[maxIdx + 1];
                            for (int i = 0; i < arr.Length; i++) arr[i] = (0, 0);
                            foreach (var iface in interfaceTraffic)
                                arr[iface.Key] = (iface.Value.In, iface.Value.Out);
                            dayStat.globalState[deviceId] = arr;
                        }

                        await globaleDatabase.AddAsync(dayStat, ct);

                        await snapshot.PauseAsync();
                        string minuteFilePath = Config.todayStatisticDataBasePath;
                        if (File.Exists(minuteFilePath))
                        {
                            File.Delete(minuteFilePath);
                            using (File.Create(minuteFilePath)) { }
                        }
                        Logger.Log("дневные записи очищены");
                        await snapshot.ResumeAsync();

                        var nextDay = now.Date.AddDays(1);
                        var delayToNext = (int)(nextDay - DateTime.Now).TotalMilliseconds + 5000;
                        await Task.Delay(delayToNext, ct);
                    }
                }
                catch (Exception e)
                {
                    Logger.Error($"проблема в ежедневной записи: {e.Message}");
                    await Task.Delay(TimeSpan.FromMinutes(5), ct);
                }
            }
        }
    }
}