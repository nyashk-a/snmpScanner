
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Channels;
using AVcontrol;
using System.Buffers;
using System.Text;

namespace MyProgram
{
    public sealed class MinuteTrafficStore : IAsyncDisposable
    {
        private readonly string _filePath;
        private readonly Channel<MinuteRecord> _channel;
        private readonly Task _writerTask;
        private readonly CancellationTokenSource _cts = new();
        private readonly int _batchSize;
        private readonly TimeSpan _flushInterval;
        private FileStream _fileStream;
        private BinaryWriter _binaryWriter;
        private readonly object _fileLock = new();

        private bool _isPaused;
        private readonly List<MinuteRecord> _pauseBuffer = new List<MinuteRecord>();

        private static readonly ConcurrentDictionary<string, DeviceMinuteState> _states = new();
        private static int _currentMinuteKey = -1;

        private static readonly ConcurrentDictionary<string, ConcurrentDictionary<ushort, (uint In, uint Out)>> _lastValues = new();

        public MinuteTrafficStore(string filePath, int batchSize = 100, int flushIntervalSeconds = 5)
        {
            _filePath = filePath;
            _batchSize = batchSize;
            _flushInterval = TimeSpan.FromSeconds(flushIntervalSeconds);

            Logger.Log($"MinuteTrafficStore initializing: filePath={filePath}, batchSize={batchSize}, flushInterval={flushIntervalSeconds}s");

            try
            {
                OpenFileStreams();
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to open file streams: {ex.Message}");
                throw;
            }

            var options = new BoundedChannelOptions(10000)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false
            };
            _channel = Channel.CreateBounded<MinuteRecord>(options);

            _writerTask = Task.Run(WriterLoopAsync);
            Logger.Log("MinuteTrafficStore initialized, writer task started.");
        }

        private void OpenFileStreams()
        {
            Logger.Log($"Opening file stream: {_filePath}");
            _fileStream = new FileStream(_filePath, FileMode.Append, FileAccess.Write, FileShare.Read, 4096, FileOptions.Asynchronous);
            _binaryWriter = new BinaryWriter(_fileStream);
            Logger.Log($"File stream opened successfully.");
        }

        private void CloseFileStreams()
        {
            Logger.Log($"Closing file streams for {_filePath}");
            try
            {
                _binaryWriter?.Flush();
                _binaryWriter?.Dispose();
                _fileStream?.Dispose();
            }
            catch (Exception ex)
            {
                Logger.Error($"Error closing file streams: {ex.Message}");
            }
            finally
            {
                _binaryWriter = null;
                _fileStream = null;
                Logger.Log("File streams closed.");
            }
        }

        public (uint In, uint Out) GetLastVal(string deviceId, ushort interfaceIdx)
        {
            try
            {
                if (_lastValues.TryGetValue(deviceId, out var deviceLast) &&
                    deviceLast.TryGetValue(interfaceIdx, out var val))
                {
                    return val;
                }
                return (0, 0);
            }
            catch (Exception ex)
            {
                Logger.Error($"GetLastVal error for device {deviceId}, iface {interfaceIdx}: {ex.Message}");
                return (0, 0);
            }
        }

        public void UpdateLastValue(string deviceId, ushort interfaceIdx, uint inAbs, uint outAbs)
        {
            var deviceLast = _lastValues.GetOrAdd(deviceId, _ => new ConcurrentDictionary<ushort, (uint In, uint Out)>());
            deviceLast[interfaceIdx] = (inAbs, outAbs);
        }

        public void AddTraffic(string deviceId, int minuteKey, ushort interfaceIdx, uint inBytes, uint outBytes)
        {
            if (string.IsNullOrEmpty(deviceId)) throw new ArgumentNullException(nameof(deviceId));
            if (minuteKey < 0 || minuteKey >= 1440) throw new ArgumentOutOfRangeException(nameof(minuteKey));

            // Только запись дельты, _lastValues не обновляем

            int oldMinute = Interlocked.Exchange(ref _currentMinuteKey, minuteKey);
            if (oldMinute != minuteKey && oldMinute != -1)
            {
                FinishMinute(oldMinute).GetAwaiter().GetResult();
            }

            var state = _states.GetOrAdd(deviceId, _ => new DeviceMinuteState());
            state.Add(interfaceIdx, inBytes, outBytes);
        }

        private async Task FinishMinute(int minuteKey)
        {
            var snapshot = _states.ToArray();
            _states.Clear();

            int recordCount = 0;
            foreach (var kv in snapshot)
            {
                var deviceId = kv.Key;
                var state = kv.Value;
                foreach (var entry in state.InterfaceTraffic)
                {
                    var record = new MinuteRecord
                    {
                        MinuteKey = minuteKey,
                        DeviceId = deviceId,
                        InterfaceIdx = entry.Key,
                        InBytes = entry.Value.In,
                        OutBytes = entry.Value.Out
                    };
                    await _channel.Writer.WriteAsync(record, _cts.Token);
                    recordCount++;
                }
            }
        }

        public async Task FinishCurrentMinuteAsync()
        {
            int minute = _currentMinuteKey;
            if (minute >= 0)
            {
                await FinishMinute(minute);
            }
        }

        public async Task PauseAsync()
        {
            lock (_fileLock)
            {
                if (_isPaused)
                {
                    Logger.Log("Pause requested but already paused.");
                    return;
                }
                _isPaused = true;
                Logger.Log("Pausing MinuteTrafficStore, closing file streams.");
                CloseFileStreams();
            }
            await Task.CompletedTask;
        }

        public async Task ResumeAsync()
        {
            lock (_fileLock)
            {
                if (!_isPaused)
                {
                    Logger.Log("Resume requested but already running.");
                    return;
                }
                Logger.Log("Resuming MinuteTrafficStore, reopening file streams.");
                OpenFileStreams();
                _isPaused = false;
            }
            await Task.CompletedTask;
        }

        private async Task WriterLoopAsync()
        {
            var reader = _channel.Reader;
            var buffer = new List<MinuteRecord>(_batchSize);
            var timer = new PeriodicTimer(_flushInterval);

            Logger.Log("Writer loop started.");

            try
            {
                while (!_cts.IsCancellationRequested)
                {
                    if (_isPaused)
                    {
                        while (reader.TryRead(out var rec))
                        {
                            _pauseBuffer.Add(rec);
                        }
                        await Task.Delay(100, _cts.Token);
                        continue;
                    }

                    if (_pauseBuffer.Count > 0)
                    {
                        await WriteBatchAsync(_pauseBuffer);
                        _pauseBuffer.Clear();
                    }

                    while (buffer.Count < _batchSize && reader.TryRead(out var record))
                    {
                        buffer.Add(record);
                    }

                    if (buffer.Count >= _batchSize || await timer.WaitForNextTickAsync(_cts.Token))
                    {
                        if (buffer.Count > 0)
                        {
                            await WriteBatchAsync(buffer);
                            buffer.Clear();
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Logger.Log("Writer loop cancelled.");
            }
            catch (Exception ex)
            {
                Logger.Error($"Writer loop error: {ex.Message}");
            }
            finally
            {
                if (buffer.Count > 0)
                {
                    Logger.Log($"Final flush: writing {buffer.Count} remaining records.");
                    await WriteBatchAsync(buffer);
                }
                Logger.Log("Writer loop finished.");
            }
        }

        private async Task WriteBatchAsync(List<MinuteRecord> batch)
        {
            Logger.Log($"Writing batch of {batch.Count} records to file.");
            try
            {
                lock (_fileLock)
                {
                    if (_fileStream == null || _binaryWriter == null)
                    {
                        Logger.Warning("File streams are null, skipping batch write.");
                        return;
                    }

                    foreach (var rec in batch)
                    {
                        _binaryWriter.Write(rec.MinuteKey);

                        int maxBytes = Encoding.UTF8.GetMaxByteCount(rec.DeviceId.Length);
                        byte[] buffer = ArrayPool<byte>.Shared.Rent(maxBytes);
                        try
                        {
                            int bytesWritten = Encoding.UTF8.GetBytes(rec.DeviceId, 0, rec.DeviceId.Length, buffer, 0);
                            _binaryWriter.Write(bytesWritten);
                            _binaryWriter.Write(buffer, 0, bytesWritten);
                        }
                        finally
                        {
                            ArrayPool<byte>.Shared.Return(buffer);
                        }

                        _binaryWriter.Write(rec.InterfaceIdx);
                        _binaryWriter.Write(rec.InBytes);
                        _binaryWriter.Write(rec.OutBytes);
                    }
                    _binaryWriter.Flush();
                }
                Logger.Log($"Batch of {batch.Count} records written successfully.");
            }
            catch (Exception ex)
            {
                Logger.Error($"Error writing batch: {ex.Message}");
                throw;
            }
            await Task.CompletedTask;
        }

        public IEnumerable<MinuteRecord> ReadAllRecords()
        {
            Logger.Log($"Reading all records from {_filePath}");
            if (!File.Exists(_filePath))
            {
                Logger.Warning($"File {_filePath} does not exist.");
                yield break;
            }

            using var fs = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            using var reader = new BinaryReader(fs);
            long count = 0;
            while (fs.Position < fs.Length)
            {
                int minuteKey = reader.ReadInt32();
                int idLen = reader.ReadInt32();
                byte[] idBytes = reader.ReadBytes(idLen);
                string deviceId = Encoding.UTF8.GetString(idBytes);
                ushort interfaceIdx = reader.ReadUInt16();
                ulong inBytes = reader.ReadUInt64();
                ulong outBytes = reader.ReadUInt64();
                count++;
                yield return new MinuteRecord { MinuteKey = minuteKey, DeviceId = deviceId, InterfaceIdx = interfaceIdx, InBytes = inBytes, OutBytes = outBytes };
            }
            Logger.Log($"Read {count} records.");
        }

        public async ValueTask DisposeAsync()
        {
            Logger.Log("Disposing MinuteTrafficStore...");
            if (_isPaused)
                await ResumeAsync();

            await FinishCurrentMinuteAsync();
            _channel.Writer.Complete();
            _cts.Cancel();
            await _writerTask;

            lock (_fileLock)
            {
                CloseFileStreams();
            }
            _cts.Dispose();
            Logger.Log("MinuteTrafficStore disposed.");
        }

        private sealed class DeviceMinuteState
        {
            public Dictionary<ushort, (ulong In, ulong Out)> InterfaceTraffic { get; } = new();

            public void Add(ushort idx, uint inBytes, uint outBytes)
            {
                if (InterfaceTraffic.TryGetValue(idx, out var existing))
                    InterfaceTraffic[idx] = (existing.In + inBytes, existing.Out + outBytes);
                else
                    InterfaceTraffic.Add(idx, (inBytes, outBytes));
            }
        }

        public struct MinuteRecord
        {
            public int MinuteKey;
            public string DeviceId;
            public ushort InterfaceIdx;
            public ulong InBytes;
            public ulong OutBytes;
        }
    }
}