using System;
using System.ComponentModel.Design.Serialization;
using System.Net;
using System.Net.Sockets;
using Lextm.SharpSnmpLib;
using Lextm.SharpSnmpLib.Messaging;

namespace MyProgram
{
    internal class Monitor
    {
        private const int PortNumber = 161;
        private const string DefaultCommunity = "public";
        private readonly List<string> _targetIpAddresses = File.ReadAllLines(Config.csvConfigPath).ToList();
        public async Task<object?> GetSnmpValueAsync(
            string ip,
            string[] oid,
            UdpClient? client = null,
            string community = DefaultCommunity,
            CancellationToken cancellationToken = default)
        {
            var list = await GetSnmpValuesAsync(ip, oid, client, community, cancellationToken);
            return list?.FirstOrDefault(); // вернёт первый элемент или null
        }

        public async Task<IList<object?>> GetSnmpValuesAsync(
            string ip,
            string[] oid,
            UdpClient? client = null,
            string community = DefaultCommunity,
            CancellationToken cancellationToken = default)
        {
            if (!IPAddress.TryParse(ip, out var ipAddress))
            {
                Logger.Error($"Invalid IP address: {ip}");
                Console.WriteLine($"Некорректный IP-адрес: {ip}");
                return null;
            }

            var endpoint = new IPEndPoint(ipAddress, PortNumber);
            var variables = oid.Select(o => new Variable(new ObjectIdentifier(o))).ToList();

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(
                new CancellationTokenSource(TimeSpan.FromMilliseconds(1000)).Token,
                cancellationToken);

            try
            {
                IList<Variable> result = await Messenger.GetAsync(
                    VersionCode.V2,
                    endpoint,
                    new OctetString(community),
                    variables,
                    cts.Token);

                return result.Select(v => ConvertSnmpData(v.Data)).ToList();
            }
            catch (SnmpException ex)
            {
                Logger.Error($"SNMP error for {ip}: {ex.Message}");
                Console.WriteLine($"Ошибка SNMP: {ex.Message}");
                return null;
            }
            catch (OperationCanceledException)
            {
                return null;
            }
            catch (Exception ex)
            {
                Logger.Error($"Unexpected error for {ip}: {ex.Message}");
                return null;
            }
        }
        private object? ConvertSnmpData(ISnmpData data)
        {
            return data switch
            {
                Integer32 i => i.ToInt32(),
                OctetString s => s.ToString(),
                TimeTicks t => t.ToUInt32(),
                Gauge32 g => g.ToUInt32(),
                Counter32 c => c.ToUInt32(),
                Counter64 c64 => c64.ToUInt64(),
                IPAddress ip => ip.ToString(),
                Null _ => null,
                _ => data.ToString() // fallback
            };
        }

        public async Task ConfigureIpListAsync(JsonlDatabase<MonitoredDevice> database)
        {
            Logger.Log($"Configuring IP list from {_targetIpAddresses.Count} addresses from {Config.csvConfigPath}");
            var semaphore = new SemaphoreSlim(300);
            var tasks = _targetIpAddresses.Select(ip => ProcessIpAsync(ip, semaphore, database));
            await Task.WhenAll(tasks);
            Logger.Log("IP list configuration completed");
            await database.SaveAsync();
        }

        private async Task ProcessIpAsync(string ip, SemaphoreSlim semaphore, JsonlDatabase<MonitoredDevice> db)
        {
            await semaphore.WaitAsync();
            try
            {
                using var udpClient = new UdpClient();
                udpClient.Client.ReceiveTimeout = 2000;
                var device = new MonitoredDevice { ID = ip };
                var openInterfaces = new List<ushort>();
                int timeout = 0;
                int now;
                for (ushort i = 1; i < 60; i++)
                {
                    now = DateTime.Now.Microsecond;
                    var response = await GetSnmpValueAsync(ip, [$"1.3.6.1.2.1.2.2.1.10.{i}"], udpClient);
                    if (response != null && response != (object)"NoSuchInstance") openInterfaces.Add(i);
                    timeout = DateTime.Now.Microsecond - now > timeout ? DateTime.Now.Microsecond - now : timeout;
                }
                device.OpenInterfaces = openInterfaces.ToArray();
                device.TimeOut = timeout;
                if (await db.GetAsync(ip) == null)
                {
                    await db.AddAsync(device);
                    Logger.Log($"Added new device {ip} with {openInterfaces.Count} interfaces");
                }
                else
                {
                    await db.UpdateAsync(device);
                    Logger.Log($"Updated device {ip}, interfaces count {openInterfaces.Count}");
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"Error processing IP {ip}: {ex.Message}");
            }
            finally
            {
                semaphore.Release();
            }
        }
    }

}