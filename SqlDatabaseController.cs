using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MessagePack;
using Microsoft.Data.Sqlite; 

namespace MyProgram
{

    public sealed class SqlDatabaseController<T> : IDatabaseController<T> where T : IHasId
    {
        private readonly string _connectionString;
        private readonly MessagePackSerializerOptions _mpOptions;
        private readonly ConcurrentDictionary<string, T> _data;
        private readonly Timer _autoSaveTimer;
        private readonly SemaphoreSlim _saveLock = new(1, 1);
        private readonly int _autoSaveIntervalMs;
        private bool _disposed;
        private bool _dirty;
        private SqliteConnection _connection;

        public SqlDatabaseController(string connectionString, int autoSaveIntervalMs = 60 * 1000)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            _autoSaveIntervalMs = autoSaveIntervalMs;
            _data = new ConcurrentDictionary<string, T>();

            _mpOptions = MessagePackSerializerOptions.Standard
                .WithResolver(MessagePack.Resolvers.ContractlessStandardResolver.Instance);

            InitializeDatabase().GetAwaiter().GetResult();
            LoadFromDatabase().GetAwaiter().GetResult();

            if (_autoSaveIntervalMs > 0)
            {
                _autoSaveTimer = new Timer(_ => Task.Run(TimedSave), null, _autoSaveIntervalMs, _autoSaveIntervalMs);
            }
        }

        private async Task InitializeDatabase()
        {
            _connection = new SqliteConnection(_connectionString);
            await _connection.OpenAsync();

            using var cmd = _connection.CreateCommand();
            cmd.CommandText = @"
                CREATE TABLE IF NOT EXISTS Data (
                    Id TEXT PRIMARY KEY,
                    IsDeleted INTEGER NOT NULL,
                    Data BLOB NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_IsDeleted ON Data(IsDeleted);
            ";
            await cmd.ExecuteNonQueryAsync();
        }

        private async Task LoadFromDatabase()
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = "SELECT Id, IsDeleted, Data FROM Data WHERE IsDeleted = 0";
            using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                try
                {
                    string id = reader.GetString(0);
                    byte[] blob = (byte[])reader.GetValue(2);
                    T? item = MessagePackSerializer.Deserialize<T>(blob, _mpOptions);
                    if (item == null || string.IsNullOrEmpty(item.ID))
                        continue;
                    item.IsDeleted = false;
                    _data[id] = item;
                }
                catch
                {
                }
            }
        }

        private async Task SaveToDatabaseAsync(CancellationToken cancellationToken)
        {
            var snapshot = _data.Values.ToArray();

            using var transaction = await _connection.BeginTransactionAsync(cancellationToken);
            using var deleteCmd = _connection.CreateCommand();
            deleteCmd.CommandText = "DELETE FROM Data";
            deleteCmd.Transaction = (SqliteTransaction)transaction;
            await deleteCmd.ExecuteNonQueryAsync(cancellationToken);

            using var insertCmd = _connection.CreateCommand();
            insertCmd.CommandText = "INSERT INTO Data (Id, IsDeleted, Data) VALUES (@id, 0, @data)";
            insertCmd.Transaction = (SqliteTransaction)transaction;
            var idParam = insertCmd.CreateParameter();
            idParam.ParameterName = "@id";
            insertCmd.Parameters.Add(idParam);
            var dataParam = insertCmd.CreateParameter();
            dataParam.ParameterName = "@data";
            insertCmd.Parameters.Add(dataParam);

            foreach (var item in snapshot)
            {
                if (item.IsDeleted) continue;
                byte[] data = MessagePackSerializer.Serialize(item, _mpOptions, cancellationToken);
                idParam.Value = item.ID;
                dataParam.Value = data;
                await insertCmd.ExecuteNonQueryAsync(cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);
        }

        private async void TimedSave()
        {
            if (_disposed || !_dirty) return;
            await SaveAsync(CancellationToken.None);
        }

        public async Task SaveAsync(CancellationToken cancellationToken = default)
        {
            if (_disposed) return;

            await _saveLock.WaitAsync(cancellationToken);
            try
            {
                await SaveToDatabaseAsync(cancellationToken);
                _dirty = false;
            }
            finally
            {
                _saveLock.Release();
            }
        }

        public Task<T?> GetAsync(string id, CancellationToken cancellationToken = default)
        {
            _data.TryGetValue(id, out T? item);
            return Task.FromResult(item);
        }

        public async Task AddAsync(T item, CancellationToken cancellationToken = default)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));
            if (string.IsNullOrEmpty(item.ID)) throw new ArgumentException("ID не может быть пустым", nameof(item.ID));

            item.IsDeleted = false;
            if (!_data.TryAdd(item.ID, item))
                throw new InvalidOperationException($"Запись с ID '{item.ID}' уже существует.");

            _dirty = true;
        }

        public Task<bool> UpdateAsync(T newItem, CancellationToken cancellationToken = default)
        {
            if (newItem == null) throw new ArgumentNullException(nameof(newItem));
            if (string.IsNullOrEmpty(newItem.ID)) throw new ArgumentException("ID не может быть пустым", nameof(newItem.ID));

            newItem.IsDeleted = false;
            bool updated = false;
            _data.AddOrUpdate(newItem.ID,
                (id) => { updated = true; return newItem; },
                (id, existing) => { updated = true; return newItem; });
            if (updated)
                _dirty = true;
            return Task.FromResult(updated);
        }

        public Task<bool> DeleteAsync(string id, CancellationToken cancellationToken = default)
        {
            bool removed = _data.TryRemove(id, out _);
            if (removed)
                _dirty = true;
            return Task.FromResult(removed);
        }

        public Task<List<T>> GetAllAsync(CancellationToken cancellationToken = default)
        {
            var result = new List<T>(_data.Values);
            return Task.FromResult(result);
        }

        public async ValueTask DisposeAsync()
        {
            await SaveAsync(CancellationToken.None);
            if (_disposed) return;
            _disposed = true;

            _autoSaveTimer?.Dispose();
            _saveLock.Dispose();
            if (_connection != null)
                await _connection.DisposeAsync();
        }
    }
}