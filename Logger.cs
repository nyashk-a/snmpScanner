using System;
using Shared.Source.tools;
using AVcontrol;

namespace MyProgram
{
        public static class Logger
    {
        private static DebugTool _debugTool;
        private static bool _initialized;

        public static void Init(string filePath)
        {
            if (_initialized) return;
            _debugTool = new DebugTool(filePath);
            _initialized = true;
        }

        public static void Log(string message)
        {
            if (!_initialized) return;
            _debugTool.Log(message + '\n');
        }

        public static void Error(string message)
        {
            if (!_initialized) return;
            _debugTool.Error(message + '\n');
        }

        public static void Warning(string message)
        {
            if (!_initialized) return;
            _debugTool.Warning(message + '\n');
        }

        public static async ValueTask DisposeAsync()
        {
            if (_debugTool != null)
                await _debugTool.DisposeAsync();
            _initialized = false;
        }
    }

}