using System;
using System.Collections.Generic;
using System.Linq;

namespace MyProgram
{
    public static class ListExtensions
    {
        public static List<List<T>> SplitIntoParts<T>(this List<T> source, int partCount)
        {
            int total = source.Count;
            int baseSize = total / partCount;
            int remainder = total % partCount;

            var result = new List<List<T>>(partCount);
            int start = 0;

            for (int i = 0; i < partCount; i++)
            {
                int currentSize = baseSize + (i < remainder ? 1 : 0);
                if (currentSize == 0)
                {
                    result.Add(new List<T>());
                }
                else
                {
                    result.Add(source.GetRange(start, currentSize));
                    start += currentSize;
                }
            }

            return result;
        }
        public static List<List<MonitoredDevice>> SplitIntoBalancedGroups(
            this List<MonitoredDevice> source,
            int partCount)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));

            if (partCount <= 0)
                throw new ArgumentOutOfRangeException(nameof(partCount), "Количество групп должно быть положительным.");

            if (partCount >= source.Count)
            {
                var sorted = source.OrderBy(d => d.TimeOut).ToList();
                var result = new List<List<MonitoredDevice>>(partCount);
                for (int i = 0; i < partCount; i++)
                {
                    result.Add(i < sorted.Count ? new List<MonitoredDevice> { sorted[i] } : new List<MonitoredDevice>());
                }
                return result;
            }

            var orderedDevices = source.OrderByDescending(d => d.TimeOut).ToList();

            var groups = new List<List<MonitoredDevice>>(partCount);
            var sums = new long[partCount];
            for (int i = 0; i < partCount; i++)
            {
                groups.Add(new List<MonitoredDevice>());
                sums[i] = 0;
            }

            foreach (var device in orderedDevices)
            {
                int minIndex = 0;
                long minSum = sums[0];
                for (int i = 1; i < partCount; i++)
                {
                    if (sums[i] < minSum)
                    {
                        minSum = sums[i];
                        minIndex = i;
                    }
                }

                groups[minIndex].Add(device);
                sums[minIndex] += device.TimeOut;
            }

            foreach (var group in groups)
            {
                group.Sort((a, b) => a.TimeOut.CompareTo(b.TimeOut));
            }

            return groups;
        }
    }
}