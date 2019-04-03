using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using server.Upload.Concurrency;
using server.Upload.Util;
using Serilog;

namespace server.Upload.Controllers.Upload {
    sealed class FileProcessor {
        private readonly ConcurrentFileWriter _writer;
        private readonly WebSocket _ws;
        private readonly string _code;
        private readonly string _fileName;
        private readonly long _targetFileSize;
        private readonly ManualResetEvent _sentFinalResponseNotification = new ManualResetEvent(false);

        public FileProcessor(WebSocket ws, long targetFileSize, string code, string fileName) {
            _ws = ws;
            _code = code;
            _fileName = fileName;
            _targetFileSize = targetFileSize;
            string dir = Path.Combine(UploadConfig.DownloadDir, code);
            Directory.CreateDirectory(dir);
            _writer = new ConcurrentFileWriter(Path.Combine(dir, fileName), targetFileSize);
            _writer.RegisterFinally(Finish);
        }

        public async Task Run() {
            try {
                _writer.RunAsync();
                for (int bytesReceived = 0; bytesReceived < _targetFileSize;) {
                    var buf = new byte[1024 * 128]; // 128 KiB

                    WebSocketReceiveResult result =
                        await _ws.ReceiveAsync(new ArraySegment<byte>(buf), CancellationToken.None);

                    bytesReceived += result.Count;
                    

                    await UploadUtil.SendResp(_ws, Resp.Ok().WithValueLong(result.Count));


                    _writer.Process(new ArraySegment<byte>(buf, 0, result.Count));
                }
            } catch (Exception e) {
                Log.Error("Error while processing file: {exception}", e);
                _writer.Cancel();
            }

            _sentFinalResponseNotification.WaitOne();
        }

        private async void Finish(bool success) {
            try {
                if (_ws.State != WebSocketState.Open) {
                    Log.Information("Upload failed.");
                    return;
                }

                if (success) {
                    await HandleSuccess();
                } else {
                    Log.Information("Upload failed.");
                    await HandleUnknownFailure();
                }
            } catch (Exception e) {
                Log.Error("Error while finishing upload: {exception}", e);
            } finally {
                _sentFinalResponseNotification.Set();
            }
        }

        private async Task HandleSuccess() {
            await UploadUtil.SendResp(_ws, Resp.Ok(MakeDownloadUrl()));
            Log.Information("Upload succeeded.");
            await _ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "Success", CancellationToken.None);
        }

        private async Task HandleUnknownFailure() {
            await UploadUtil.SendResp(_ws, Resp.InternalError());
            await _ws.CloseAsync(WebSocketCloseStatus.InternalServerError, "Unknown Error", CancellationToken.None);
        }

        private string MakeDownloadUrl() {
            string protocol = UploadConfig.WebsiteProtocol;
            string domain = UploadConfig.WebsiteDomain;
            string downloadDir = Uri.EscapeDataString(UploadConfig.DownloadDir);
            string code = Uri.EscapeDataString(_code);
            string fileName = Uri.EscapeDataString(_fileName);
            return $"{protocol}{domain}/{downloadDir}/{code}/{fileName}";
        }
    }
}
