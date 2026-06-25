using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using MessagePack;

namespace MyProgram
{
    public interface IHasId
    {
        string ID { get; set; }
        bool IsDeleted { get; set; }
    }

    /// <summary>
    /// Хранилище объектов типа T в бинарном файле с префиксом длины.
    /// Формат файла: последовательность блоков [Int32 длина][байты MessagePack].
    /// </summary>
    public sealed class JsonlDatabase<T> : IAsyncDisposable where T : IHasId
    {
        private readonly string _filePath;
        private readonly MessagePackSerializerOptions _mpOptions;
        private readonly ConcurrentDictionary<string, T> _data;
        private readonly Timer _autoSaveTimer;
        private readonly SemaphoreSlim _saveLock = new(1, 1);
        private readonly int _autoSaveIntervalMs;
        private bool _disposed;
        private bool _dirty;

        public JsonlDatabase(string filePath, int autoSaveIntervalMs = 60 * 1000)
        {
            _filePath = filePath ?? throw new ArgumentNullException(nameof(filePath));
            _autoSaveIntervalMs = autoSaveIntervalMs;
            _data = new ConcurrentDictionary<string, T>();

            // Настройки MessagePack: игнорируем свойства со значением null, используем стандартные соглашения
            _mpOptions = MessagePackSerializerOptions.Standard
                .WithResolver(MessagePack.Resolvers.ContractlessStandardResolver.Instance);

            LoadFromFile();

            if (_autoSaveIntervalMs > 0)
            {
                _autoSaveTimer = new Timer(_ => Task.Run(TimedSave), null, _autoSaveIntervalMs, _autoSaveIntervalMs);
            }
        }

        private void LoadFromFile()
        {
            if (!File.Exists(_filePath))
            {
                // Создаём пустой файл, если его нет
                using var ffs = new FileStream(_filePath, FileMode.Create, FileAccess.Write);
                return;
            }

            using var fs = new FileStream(_filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            using var reader = new BinaryReader(fs);

            while (fs.Position < fs.Length)
            {
                try
                {
                    // Читаем длину блока
                    int length = reader.ReadInt32();
                    if (length <= 0 || length > fs.Length - fs.Position)
                        break; // повреждённые данные – выходим

                    byte[] data = reader.ReadBytes(length);
                    if (data.Length != length)
                        break;

                    T? item = MessagePackSerializer.Deserialize<T>(data, _mpOptions);
                    if (item == null || string.IsNullOrEmpty(item.ID))
                        continue;

                    if (item.IsDeleted)
                        continue;

                    _data[item.ID] = item;
                }
                catch
                {
                    // При ошибке чтения прерываем загрузку
                    break;
                }
            }
        }

        private async Task SaveToFileAsync(CancellationToken cancellationToken = default)
        {
            var snapshot = _data.Values.ToArray();

            string tempFile = _filePath + ".tmp";
            try
            {
                using var fs = new FileStream(tempFile, FileMode.Create, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous);
                using var writer = new BinaryWriter(fs);

                foreach (var item in snapshot)
                {
                    if (item.IsDeleted)
                        continue;

                    byte[] data = MessagePackSerializer.Serialize(item, _mpOptions, cancellationToken);
                    // Записываем длину, затем байты
                    writer.Write(data.Length);
                    writer.Write(data);
                }
                await writer.BaseStream.FlushAsync(cancellationToken);
            }
            catch
            {
                if (File.Exists(tempFile))
                    File.Delete(tempFile);
                throw;
            }

            // Атомарная замена
            File.Replace(tempFile, _filePath, null);
        }

        private async void TimedSave()
        {
            if (_disposed) return;
            if (!_dirty) return;
            await SaveAsync(CancellationToken.None);
        }

        public async Task SaveAsync(CancellationToken cancellationToken = default)
        {
            if (_disposed) return;

            await _saveLock.WaitAsync(cancellationToken);
            try
            {
                await SaveToFileAsync(cancellationToken);
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
        }
    }
}