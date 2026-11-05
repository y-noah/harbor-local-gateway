// Windows .NET Framework 4.8 helper. Device private keys remain in Windows CNG;
// HttpClient consumes certificate handles and never exports a private-key PEM.
using System;
using System.IO;
using System.IO.Pipes;
using System.Net;
using System.Net.Http;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.AccessControl;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Security.Principal;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System.Management;
using System.Web.Script.Serialization;
using Microsoft.Win32;

public static class HarborDevice {
  static readonly JavaScriptSerializer Json = new JavaScriptSerializer { MaxJsonLength = 1048576 };
  static readonly HashSet<string> Hop = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "Host", "Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "TE", "Trailer", "Transfer-Encoding", "Upgrade", "X-Harbor-Local-Token" };
  static readonly byte[] Entropy = Encoding.UTF8.GetBytes("harbor-device-helper-v1");
  const long MaxBody = 134217728;
  static string Stage="startup";
  sealed class DeviceError : Exception { public DeviceError(string code) : base(code) {} }
  public sealed class State {
    public int Version = 1; public string Gateway; public string CaFile; public string DeviceId; public string Scope;
    public string KeyName; public string KeyProvider; public string KeyProtection; public string CertificateThumbprint; public List<string> CertificateThumbprints = new List<string>();
    public string EnrollmentProtected; public string LocalTokenProtected; public string Fingerprint; public string InstallationFingerprint;
    public string FingerprintSource; public string PipeName; public int Port = 43128;
  }
  static string Hash(byte[] bytes) { using (var sha = SHA256.Create()) return BitConverter.ToString(sha.ComputeHash(bytes)).Replace("-", "").ToLowerInvariant(); }
  static string RandomSecret() { var bytes = new byte[32]; using (var rng = RandomNumberGenerator.Create()) rng.GetBytes(bytes); return Convert.ToBase64String(bytes).TrimEnd('=').Replace('+','-').Replace('/','_'); }
  static string Protect(string value) { return Convert.ToBase64String(ProtectedData.Protect(Encoding.UTF8.GetBytes(value), Entropy, DataProtectionScope.CurrentUser)); }
  static string Unprotect(string value) { return Encoding.UTF8.GetString(ProtectedData.Unprotect(Convert.FromBase64String(value), Entropy, DataProtectionScope.CurrentUser)); }
  static void Restrict(string path, bool directory) {
    var sid = WindowsIdentity.GetCurrent().User;
    if (directory) { var acl = new DirectorySecurity(); acl.SetOwner(sid); acl.SetAccessRuleProtection(true,false); acl.AddAccessRule(new FileSystemAccessRule(sid,FileSystemRights.FullControl,InheritanceFlags.ContainerInherit|InheritanceFlags.ObjectInherit,PropagationFlags.None,AccessControlType.Allow)); new DirectoryInfo(path).SetAccessControl(acl); }
    else { var acl = new FileSecurity(); acl.SetOwner(sid); acl.SetAccessRuleProtection(true,false); acl.AddAccessRule(new FileSystemAccessRule(sid,FileSystemRights.FullControl,AccessControlType.Allow)); new FileInfo(path).SetAccessControl(acl); }
  }
  static State Load(string path) { if (!File.Exists(path)) throw new DeviceError("device_state_missing"); var state=Json.Deserialize<State>(File.ReadAllText(path,Encoding.UTF8)); if(state==null||state.Version!=1)throw new DeviceError("device_state_invalid"); ValidateGateway(state.Gateway);return state; }
  static void Save(string path, State value) { var full=Path.GetFullPath(path);Directory.CreateDirectory(Path.GetDirectoryName(full));Restrict(Path.GetDirectoryName(full),true);var temp=full+".tmp-"+Guid.NewGuid().ToString("N");try{File.WriteAllText(temp,Json.Serialize(value),new UTF8Encoding(false));Restrict(temp,false);if(File.Exists(full))File.Replace(temp,full,null);else File.Move(temp,full);Restrict(full,false);}finally{if(File.Exists(temp))File.Delete(temp);} }
  static Uri ValidateGateway(string value) { Uri uri; if(!Uri.TryCreate(value,UriKind.Absolute,out uri)||uri.Scheme!="https"||uri.UserInfo!=""||uri.Query!=""||uri.Fragment!=""||uri.AbsolutePath!="/")throw new DeviceError("device_gateway_requires_https_origin");return uri; }
  static string Option(string[] args,string key,string fallback=null) { var index=Array.IndexOf(args,key);if(index<0)return fallback;if(index+1>=args.Length||args[index+1].StartsWith("--"))throw new DeviceError("device_arguments_invalid");return args[index+1]; }
  static string StatePath(string[] args) { return Path.GetFullPath(Option(args,"--state",Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),"HarborDevice","state.json"))); }
  static string Fingerprint(string scope,string raw) { if(raw==null)return null;var normalized=raw.Trim().ToLowerInvariant().Replace("-","");if(!System.Text.RegularExpressions.Regex.IsMatch(normalized,"^[a-f0-9]{32}$")||normalized.All(c=>c=='0')||normalized.All(c=>c=='f'))return null;return Hash(Encoding.UTF8.GetBytes("harbor-device-fingerprint-v1\0"+scope+"\0"+normalized)); }
  static void Capture(State state) {
    string uuid=null,machine=null;try{using(var search=new ManagementObjectSearcher("SELECT UUID FROM Win32_ComputerSystemProduct"))foreach(ManagementObject item in search.Get()){uuid=Convert.ToString(item["UUID"]);item.Dispose();break;}}catch{}
    try{using(var key=Registry.LocalMachine.OpenSubKey(@"SOFTWARE\Microsoft\Cryptography"))machine=Convert.ToString(key.GetValue("MachineGuid"));}catch{}
    state.Fingerprint=Fingerprint(state.Scope,uuid);state.InstallationFingerprint=Fingerprint(state.Scope,machine);state.FingerprintSource=state.Fingerprint==null?"unavailable":"smbios_uuid";
  }
  static CngKey CreateKey(string name,bool allowSoftware,out string protection,Action<CngProvider> beforeCreate) {
    var options=new CngKeyCreationParameters { ExportPolicy=CngExportPolicies.None,KeyUsage=CngKeyUsages.Signing,KeyCreationOptions=CngKeyCreationOptions.None,Provider=new CngProvider("Microsoft Platform Crypto Provider") };
    options.Parameters.Add(new CngProperty("Length",BitConverter.GetBytes(2048),CngPropertyOptions.None));
    beforeCreate(options.Provider);try{var hardware=CngKey.Create(CngAlgorithm.Rsa,name,options);protection="platform-nonexportable";return hardware;}catch(CryptographicException){if(!allowSoftware)throw new DeviceError("device_hardware_key_unavailable_use_explicit_software_mode");}
    options.Provider=CngProvider.MicrosoftSoftwareKeyStorageProvider;beforeCreate(options.Provider);var software=CngKey.Create(CngAlgorithm.Rsa,name,options);protection="software-nonexportable";return software;
  }
  static RSACng OpenKey(State state) { var key=CngKey.Open(state.KeyName,new CngProvider(state.KeyProvider));if(key.ExportPolicy!=CngExportPolicies.None){key.Dispose();throw new DeviceError("device_key_export_policy_invalid");}return new RSACng(key); }
  static string Pem(string label,byte[] der) { var value=Convert.ToBase64String(der);var lines=new List<string>();for(int i=0;i<value.Length;i+=64)lines.Add(value.Substring(i,Math.Min(64,value.Length-i)));return "-----BEGIN "+label+"-----\n"+String.Join("\n",lines)+"\n-----END "+label+"-----\n"; }
  static string Quote(string value) { if(value==null)return "null";var text=new StringBuilder("\"");foreach(char c in value){switch(c){case '"':text.Append("\\\"");break;case '\\':text.Append("\\\\");break;case '\b':text.Append("\\b");break;case '\f':text.Append("\\f");break;case '\n':text.Append("\\n");break;case '\r':text.Append("\\r");break;case '\t':text.Append("\\t");break;default:if(c<32)text.Append("\\u"+((int)c).ToString("x4"));else text.Append(c);break;}}return text.Append('"').ToString(); }
  static string EnrollmentMessage(Dictionary<string,object> p) { return "{\"protocol\":\"harbor-device-enrollment-v1\",\"challenge\":"+Quote((string)p["challenge"])+",\"nonce\":"+Quote((string)p["nonce"])+",\"scope\":"+Quote((string)p["scope"])+",\"csr_sha256\":"+Quote(Hash(Encoding.UTF8.GetBytes((string)p["csr"])))+",\"name\":"+Quote((string)p["name"])+",\"system\":"+Quote((string)p["system"])+",\"fingerprint\":"+Quote((string)p["fingerprint"])+",\"installation_fingerprint\":"+Quote((string)p["installation_fingerprint"])+",\"fingerprint_source\":"+Quote((string)p["fingerprint_source"])+"}"; }
  static X509Certificate2 Certificate(State state) { if(String.IsNullOrEmpty(state.CertificateThumbprint))throw new DeviceError("device_pending_approval");using(var store=new X509Store(StoreName.My,StoreLocation.CurrentUser)){store.Open(OpenFlags.ReadOnly);var values=store.Certificates.Find(X509FindType.FindByThumbprint,state.CertificateThumbprint,false);if(values.Count!=1||!values[0].HasPrivateKey||values[0].NotAfter.ToUniversalTime()<=DateTime.UtcNow)throw new DeviceError("device_certificate_missing_or_expired");return new X509Certificate2(values[0]);} }
  static HttpClient Client(State state,bool authenticated) {
    var handler=new HttpClientHandler { AllowAutoRedirect=false,AutomaticDecompression=DecompressionMethods.None,UseProxy=false,ClientCertificateOptions=ClientCertificateOption.Manual,CheckCertificateRevocationList=true,SslProtocols=SslProtocols.Tls12 };
    if(authenticated)handler.ClientCertificates.Add(Certificate(state));
    if(!String.IsNullOrEmpty(state.CaFile)){
      var root=new X509Certificate2(File.ReadAllBytes(state.CaFile));
      handler.ServerCertificateCustomValidationCallback=(message,certificate,chain,errors)=>{
        if((errors&SslPolicyErrors.RemoteCertificateNameMismatch)!=0||(errors&SslPolicyErrors.RemoteCertificateNotAvailable)!=0)return false;
        using(var validation=new X509Chain()){validation.ChainPolicy.ExtraStore.Add(root);validation.ChainPolicy.RevocationMode=X509RevocationMode.NoCheck;validation.ChainPolicy.VerificationFlags=X509VerificationFlags.AllowUnknownCertificateAuthority;if(!validation.Build(certificate))return false;if(validation.ChainElements.Count==0)return false;var anchor=validation.ChainElements[validation.ChainElements.Count-1].Certificate;if(Hash(anchor.RawData)!=Hash(root.RawData))return false;return validation.ChainStatus.All(status=>status.Status==X509ChainStatusFlags.NoError||status.Status==X509ChainStatusFlags.UntrustedRoot);}
      };
    }
    return new HttpClient(handler) { Timeout=authenticated?Timeout.InfiniteTimeSpan:TimeSpan.FromSeconds(30),MaxResponseContentBufferSize=1048576 };
  }
#if HARBOR_CAPACITY_OBSERVATION
  static bool CapacityObservationEnabled;
#endif
  static async Task<Dictionary<string,object>> Post(HttpClient client,State state,string path,object payload) {
#if HARBOR_CAPACITY_OBSERVATION
    // The capacity-only build handshakes over owned stdin/stdout before sending.
    // Production builds omit this block; no identity or request content is emitted.
    string capacityId=null;int capacityStatus=0;bool capacityComplete=false;
    if(CapacityObservationEnabled){
      capacityId=Guid.NewGuid().ToString();Console.WriteLine(Json.Serialize(new {capacity_http="before",id=capacityId,method="POST",path=path}));Console.Out.Flush();
      string trace=Console.ReadLine();Guid parsed;if(!Guid.TryParseExact(trace,"D",out parsed))throw new DeviceError("capacity_trace_handshake_failed");
      client.DefaultRequestHeaders.Remove("X-Capacity-Trace");client.DefaultRequestHeaders.TryAddWithoutValidation("X-Capacity-Trace",trace);
    }
    try{
#endif
    using(var response=await client.PostAsync(new Uri(ValidateGateway(state.Gateway),path),new StringContent(Json.Serialize(payload),Encoding.UTF8,"application/json"))){
#if HARBOR_CAPACITY_OBSERVATION
      capacityStatus=(int)response.StatusCode;
#endif
      var text=await response.Content.ReadAsStringAsync();
#if HARBOR_CAPACITY_OBSERVATION
      capacityComplete=true;
#endif
      if(text.Length>262144)throw new DeviceError("device_response_too_large");var value=Json.Deserialize<Dictionary<string,object>>(text);if(!response.IsSuccessStatusCode){string code="device_http_"+(int)response.StatusCode;if(value!=null&&value.ContainsKey("error")){var error=value["error"] as Dictionary<string,object>;object reason;if(error!=null&&error.TryGetValue("code",out reason)&&System.Text.RegularExpressions.Regex.IsMatch(Convert.ToString(reason),"^[a-z][a-z0-9_]{0,79}$"))code=Convert.ToString(reason);}throw new DeviceError(code);}return value;}
#if HARBOR_CAPACITY_OBSERVATION
    }finally{if(capacityId!=null){Console.WriteLine(Json.Serialize(new {capacity_http="terminal",id=capacityId,status=capacityStatus,complete=capacityComplete}));Console.Out.Flush();}}
#endif
  }
  static async Task Prepare(string[] args) {
    var path=StatePath(args);if(File.Exists(path))throw new DeviceError("device_state_already_exists");var state=new State { Gateway=Option(args,"--gateway"),CaFile=Option(args,"--ca-file"),KeyName="harbor-device-"+Guid.NewGuid().ToString("N"),PipeName="HarborDevice-"+Guid.NewGuid().ToString("N"),Port=int.Parse(Option(args,"--port","43128")),LocalTokenProtected=Protect(RandomSecret()) };ValidateGateway(state.Gateway);if(state.Port<1024||state.Port>65535)throw new DeviceError("device_port_invalid");
    var invitation=Console.ReadLine();if(String.IsNullOrEmpty(invitation)||invitation.Length>200)throw new DeviceError("device_invitation_required_on_stdin");bool saved=false;CngKey created=null;
    try{Stage="create_key";string protection;created=CreateKey(state.KeyName,Array.IndexOf(args,"--allow-software-key")>=0,out protection,provider=>{state.KeyProvider=provider.Provider;Save(path,state);});state.KeyProvider=created.Provider.Provider;state.KeyProtection=protection;
      Stage="create_enrollment_client";using(var rsa=new RSACng(created))using(var client=Client(state,false)){
        Stage="enrollment_challenge";var challenge=await Post(client,state,"/api/device/enroll/challenge",new { invitation=invitation });state.Scope=Convert.ToString(challenge["scope"]);Stage="capture_hardware";Capture(state);
        Stage="create_csr";var csr=Pem("CERTIFICATE REQUEST",new CertificateRequest("CN=Harbor Device Enrollment",rsa,HashAlgorithmName.SHA256,RSASignaturePadding.Pkcs1).CreateSigningRequest());var payload=new Dictionary<string,object> { {"invitation",invitation},{"challenge",Convert.ToString(challenge["challenge"])},{"nonce",Convert.ToString(challenge["nonce"])},{"scope",state.Scope},{"csr",csr},{"name",Option(args,"--name",Environment.MachineName+" ("+Environment.UserName+")")},{"system",Environment.OSVersion.VersionString+" / shared Windows-WSL helper"},{"fingerprint",state.Fingerprint},{"installation_fingerprint",state.InstallationFingerprint},{"fingerprint_source",state.FingerprintSource} };payload["signature"]=Convert.ToBase64String(rsa.SignData(Encoding.UTF8.GetBytes(EnrollmentMessage(payload)),HashAlgorithmName.SHA256,RSASignaturePadding.Pkcs1));
        Stage="enrollment_register";var registration=await Post(client,state,"/api/device/enroll/register",payload);state.DeviceId=Convert.ToString(registration["device_id"]);state.EnrollmentProtected=Protect(Convert.ToString(registration["enrollment_token"]));Stage="save_state";Save(path,state);saved=true;Console.WriteLine(Json.Serialize(new { state="pending",key_protection=protection,hardware=state.FingerprintSource }));
      }
    }finally{if(!saved){try{if(created!=null)created.Delete();else if(!String.IsNullOrEmpty(state.KeyProvider)){var provider=new CngProvider(state.KeyProvider);if(CngKey.Exists(state.KeyName,provider))using(var leftover=CngKey.Open(state.KeyName,provider))leftover.Delete();}File.Delete(path);}catch{}}if(created!=null)created.Dispose();}
  }
  static async Task Renew(string[] args) {
    var path=StatePath(args);var state=Load(path);var invitation=Console.ReadLine();if(String.IsNullOrEmpty(invitation)||invitation.Length>200)throw new DeviceError("device_invitation_required_on_stdin");
    using(var rsa=OpenKey(state))using(var client=Client(state,false)){
      var challenge=await Post(client,state,"/api/device/enroll/challenge",new { invitation=invitation });if(Convert.ToString(challenge["scope"])!=state.Scope)throw new DeviceError("device_instance_changed");Capture(state);
      var csr=Pem("CERTIFICATE REQUEST",new CertificateRequest("CN=Harbor Device Renewal",rsa,HashAlgorithmName.SHA256,RSASignaturePadding.Pkcs1).CreateSigningRequest());var payload=new Dictionary<string,object> { {"invitation",invitation},{"challenge",Convert.ToString(challenge["challenge"])},{"nonce",Convert.ToString(challenge["nonce"])},{"scope",state.Scope},{"csr",csr},{"name",Option(args,"--name",Environment.MachineName+" ("+Environment.UserName+")")},{"system",Environment.OSVersion.VersionString+" / shared Windows-WSL helper"},{"fingerprint",state.Fingerprint},{"installation_fingerprint",state.InstallationFingerprint},{"fingerprint_source",state.FingerprintSource} };payload["signature"]=Convert.ToBase64String(rsa.SignData(Encoding.UTF8.GetBytes(EnrollmentMessage(payload)),HashAlgorithmName.SHA256,RSASignaturePadding.Pkcs1));
      var registration=await Post(client,state,"/api/device/enroll/register",payload);if(Convert.ToString(registration["device_id"])!=state.DeviceId)throw new DeviceError("device_renewal_identity_changed");state.EnrollmentProtected=Protect(Convert.ToString(registration["enrollment_token"]));Save(path,state);Console.WriteLine(Json.Serialize(new{state=Convert.ToString(registration["state"]),key_protection=state.KeyProtection}));
    }
  }
  static async Task Complete(string[] args) {
    var path=StatePath(args);var state=Load(path);using(var client=Client(state,false)){var result=await Post(client,state,"/api/device/enroll/status",new { enrollment_token=Unprotect(state.EnrollmentProtected) });if(Convert.ToString(result["state"])!="approved"){Console.WriteLine(Json.Serialize(new {state=Convert.ToString(result["state"])}));return;}
      var certificate=new X509Certificate2(Encoding.ASCII.GetBytes(Convert.ToString(result["certificate"])));using(var key=OpenKey(state))using(var combined=RSACertificateExtensions.CopyWithPrivateKey(certificate,key))using(var store=new X509Store(StoreName.My,StoreLocation.CurrentUser)){store.Open(OpenFlags.ReadWrite);store.Add(combined);if(state.CertificateThumbprints==null)state.CertificateThumbprints=new List<string>();if(!String.IsNullOrEmpty(state.CertificateThumbprint)&&!state.CertificateThumbprints.Contains(state.CertificateThumbprint))state.CertificateThumbprints.Add(state.CertificateThumbprint);state.CertificateThumbprint=combined.Thumbprint;if(!state.CertificateThumbprints.Contains(state.CertificateThumbprint))state.CertificateThumbprints.Add(state.CertificateThumbprint);Save(path,state);}Console.WriteLine(Json.Serialize(new {state="approved",key_protection=state.KeyProtection,hardware=state.FingerprintSource}));}
  }
  static bool AllowedPath(string value) { Uri uri;if(!Uri.TryCreate(value,UriKind.Relative,out uri)||!value.StartsWith("/")||value.StartsWith("//")||value.Contains("\\")||value.Any(c=>c<32))return false;var path=value.Split('?')[0];var canonical=new Uri(new Uri("https://localhost"),value);if(canonical.AbsolutePath!=path)return false;return path=="/api/me"||path=="/v1/models"||path.StartsWith("/v1/codex/")||path=="/v1/responses"||path.StartsWith("/v1/images/")||path=="/v1/chat/completions"||path=="/v1/platform/models"||path=="/v1/platform/chat/completions"; }
  static void AddHeaders(HttpRequestMessage request,Dictionary<string,string> headers,State state) { foreach(var pair in headers){if(Hop.Contains(pair.Key)||pair.Key.StartsWith("X-Harbor-Device",StringComparison.OrdinalIgnoreCase)||pair.Key.Equals("X-Harbor-Hardware",StringComparison.OrdinalIgnoreCase)||pair.Key.Equals("X-Harbor-Installation",StringComparison.OrdinalIgnoreCase)||pair.Key.Equals("Content-Length",StringComparison.OrdinalIgnoreCase))continue;if(!request.Headers.TryAddWithoutValidation(pair.Key,pair.Value)&&request.Content!=null)request.Content.Headers.TryAddWithoutValidation(pair.Key,pair.Value);}if(state.Fingerprint!=null)request.Headers.TryAddWithoutValidation("X-Harbor-Hardware",state.Fingerprint);if(state.InstallationFingerprint!=null)request.Headers.TryAddWithoutValidation("X-Harbor-Installation",state.InstallationFingerprint); }
  static bool SecureLocal(Dictionary<string,string> headers,string localToken) { string token,host;if(!headers.TryGetValue("X-Harbor-Local-Token",out token)||token!=localToken||!headers.TryGetValue("Host",out host)||!(host.StartsWith("127.0.0.1:",StringComparison.Ordinal)||host.StartsWith("localhost:",StringComparison.Ordinal)))return false;return !headers.Keys.Any(key=>key.Equals("Origin",StringComparison.OrdinalIgnoreCase)||(key.Equals("Sec-Fetch-Site",StringComparison.OrdinalIgnoreCase)||key.Equals("Sec-Fetch-Dest",StringComparison.OrdinalIgnoreCase)||key.Equals("Sec-Fetch-User",StringComparison.OrdinalIgnoreCase))); }
  sealed class LimitedContent : HttpContent {
    readonly Stream source;readonly long expected;
    public LimitedContent(Stream value,long length){source=value;expected=length;}
    protected override bool TryComputeLength(out long length){length=expected;return expected>=0;}
    protected override async Task SerializeToStreamAsync(Stream target,TransportContext context){long total=0;var bytes=new byte[65536];for(;;){int count=await source.ReadAsync(bytes,0,bytes.Length);if(count==0)break;total+=count;if(total>MaxBody)throw new DeviceError("device_request_too_large");await target.WriteAsync(bytes,0,count);}if(expected>=0&&expected!=total)throw new DeviceError("device_body_incomplete");}
  }
  static async Task HandleHttp(HttpListenerContext context,HttpClient client,State state,string localToken,CancellationToken stop) {
    var input=context.Request;var output=context.Response;var headers=input.Headers.AllKeys.ToDictionary(key=>key,key=>input.Headers[key],StringComparer.OrdinalIgnoreCase);
    try{if(!SecureLocal(headers,localToken)||!AllowedPath(input.RawUrl)){output.StatusCode=403;return;}if(input.ContentLength64>MaxBody){output.StatusCode=413;return;}
      using(var request=new HttpRequestMessage(new HttpMethod(input.HttpMethod),new Uri(ValidateGateway(state.Gateway),input.RawUrl))){if(input.HasEntityBody)request.Content=new LimitedContent(input.InputStream,input.ContentLength64);AddHeaders(request,headers,state);
        using(var response=await client.SendAsync(request,HttpCompletionOption.ResponseHeadersRead,stop)){output.StatusCode=(int)response.StatusCode;foreach(var header in response.Headers.Concat(response.Content.Headers)){if(Hop.Contains(header.Key)||header.Key.Equals("Content-Length",StringComparison.OrdinalIgnoreCase))continue;try{output.Headers[header.Key]=String.Join(", ",header.Value);}catch{}}
          output.SendChunked=true;using(var source=await response.Content.ReadAsStreamAsync()){var bytes=new byte[65536];for(;;){int count=await source.ReadAsync(bytes,0,bytes.Length,stop);if(count==0)break;await output.OutputStream.WriteAsync(bytes,0,count,stop);await output.OutputStream.FlushAsync(stop);}}
        }
      }
    }catch{try{output.Abort();}catch{}}finally{try{output.Close();}catch{}}
  }
  sealed class BufferedInput : Stream {
    readonly Stream source;readonly byte[] buffer=new byte[8192];int start,end;
    public BufferedInput(Stream value){source=value;}
    public async Task<string> Line(int maximum){var line=new List<byte>();for(;;){if(start==end){end=await source.ReadAsync(buffer,0,buffer.Length);start=0;if(end==0)throw new DeviceError("device_local_incomplete_header");}byte value=buffer[start++];if(value==10){if(line.Count==0||line[line.Count-1]!=13)throw new DeviceError("device_local_header_invalid");line.RemoveAt(line.Count-1);return Encoding.ASCII.GetString(line.ToArray());}line.Add(value);if(line.Count>maximum)throw new DeviceError("device_local_header_too_large");}}
    public override async Task<int> ReadAsync(byte[] output,int offset,int count,CancellationToken token){if(start<end){int copied=Math.Min(count,end-start);Buffer.BlockCopy(buffer,start,output,offset,copied);start+=copied;return copied;}return await source.ReadAsync(output,offset,count,token);}
    public override int Read(byte[] output,int offset,int count){return ReadAsync(output,offset,count,CancellationToken.None).GetAwaiter().GetResult();}public override bool CanRead{get{return true;}}public override bool CanSeek{get{return false;}}public override bool CanWrite{get{return false;}}public override long Length{get{throw new NotSupportedException();}}public override long Position{get{throw new NotSupportedException();}set{throw new NotSupportedException();}}public override void Flush(){}public override long Seek(long value,SeekOrigin origin){throw new NotSupportedException();}public override void SetLength(long value){throw new NotSupportedException();}public override void Write(byte[] value,int offset,int count){throw new NotSupportedException();}
  }
  sealed class LocalRequest {
    public string Method,Path;public Dictionary<string,string> Headers;public long Length;public bool Chunked;
  }
  static async Task<LocalRequest> ReadLocal(BufferedInput input){
    string line=await input.Line(8192);var first=line.Split(' ');if(first.Length!=3||!System.Text.RegularExpressions.Regex.IsMatch(first[0],"^[A-Z]{3,12}$")||first[2]!="HTTP/1.1"||!AllowedPath(first[1]))throw new DeviceError("device_local_request_invalid");var headers=new Dictionary<string,string>(StringComparer.OrdinalIgnoreCase);int bytes=line.Length;
    for(;;){line=await input.Line(8192);bytes+=line.Length+2;if(bytes>32768)throw new DeviceError("device_local_header_too_large");if(line.Length==0)break;int index=line.IndexOf(':');if(index<=0||Char.IsWhiteSpace(line[0]))throw new DeviceError("device_local_header_invalid");string name=line.Substring(0,index),value=line.Substring(index+1).Trim();if(!System.Text.RegularExpressions.Regex.IsMatch(name,"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")||value.Any(c=>c<32&&c!=9)||headers.ContainsKey(name))throw new DeviceError("device_local_header_invalid");headers.Add(name,value);}
    string encoded,length;bool chunked=headers.TryGetValue("Transfer-Encoding",out encoded);long size=0;if(chunked&&(!encoded.Equals("chunked",StringComparison.OrdinalIgnoreCase)||headers.ContainsKey("Content-Length")))throw new DeviceError("device_local_framing_invalid");if(headers.TryGetValue("Content-Length",out length)&&(!System.Text.RegularExpressions.Regex.IsMatch(length,"^[0-9]+$")||!Int64.TryParse(length,out size)||size>MaxBody))throw new DeviceError("device_request_too_large");return new LocalRequest{Method=first[0],Path=first[1],Headers=headers,Length=chunked?-1:size,Chunked=chunked};
  }
  sealed class LocalContent : HttpContent {
    readonly BufferedInput source;readonly LocalRequest request;readonly CancellationToken token;
    public LocalContent(BufferedInput value,LocalRequest metadata,CancellationToken cancellation){source=value;request=metadata;token=cancellation;}
    protected override bool TryComputeLength(out long length){length=request.Length;return !request.Chunked;}
    protected override async Task SerializeToStreamAsync(Stream target,TransportContext context){long total=0,remaining=request.Length;var bytes=new byte[65536];for(;;){long count=remaining;if(request.Chunked){var line=await source.Line(128);if(line.Contains(';')||!Int64.TryParse(line,System.Globalization.NumberStyles.AllowHexSpecifier,System.Globalization.CultureInfo.InvariantCulture,out count)||count<0)throw new DeviceError("device_chunk_invalid");if(count==0){if(await source.Line(8192)!="")throw new DeviceError("device_trailers_unsupported");break;}}if(count==0)break;if(total+count>MaxBody)throw new DeviceError("device_request_too_large");while(count>0){int read=await source.ReadAsync(bytes,0,(int)Math.Min(bytes.Length,count),token);if(read==0)throw new DeviceError("device_body_incomplete");await target.WriteAsync(bytes,0,read,token);count-=read;total+=read;if(!request.Chunked)remaining-=read;}if(request.Chunked&&await source.Line(2)!="")throw new DeviceError("device_chunk_invalid");if(!request.Chunked&&remaining==0)break;}}
  }
  static async Task ErrorResponse(Stream target,int status,string code){var bytes=Encoding.UTF8.GetBytes(Json.Serialize(new{error=new{code=code,message=code}}));var header=Encoding.ASCII.GetBytes("HTTP/1.1 "+status+" Error\r\nContent-Type: application/json\r\nContent-Length: "+bytes.Length+"\r\nConnection: close\r\n\r\n");await target.WriteAsync(header,0,header.Length);await target.WriteAsync(bytes,0,bytes.Length);}
  static async Task HandleTcp(TcpClient local,HttpClient client,State state,string localToken,int headerTimeout,CancellationToken stopping){
    using(local)using(var cancelled=CancellationTokenSource.CreateLinkedTokenSource(stopping))using(var shutdown=stopping.Register(()=>local.Close())){
      var network=local.GetStream();bool started=false;string localError=null;int localStatus=503;var finished=new TaskCompletionSource<bool>();
#if HARBOR_CAPACITY_OBSERVATION
      CapacityDeliveryObservation.Probe capacityDelivery=null;bool capacityComplete=false;string capacityTrace=null;
#endif
      var monitor=Task.Run(async()=>{while(!finished.Task.IsCompleted){try{if(local.Client.Poll(0,SelectMode.SelectRead)&&local.Client.Available==0){cancelled.Cancel();break;}}catch{cancelled.Cancel();break;}await Task.Delay(100);}});
      try{var input=new BufferedInput(network);LocalRequest metadata;using(var deadline=CancellationTokenSource.CreateLinkedTokenSource(cancelled.Token)){deadline.CancelAfter(headerTimeout);using(deadline.Token.Register(()=>local.Close()))metadata=await ReadLocal(input);}if(!SecureLocal(metadata.Headers,localToken)||metadata.Headers["Host"]!="127.0.0.1:"+state.Port){await ErrorResponse(network,403,"device_local_unauthorized");return;}
#if HARBOR_CAPACITY_OBSERVATION
        capacityTrace=CapacityDeliveryObservation.AuthenticatedTrace(metadata.Headers);
#endif
        using(var outgoing=new HttpRequestMessage(new HttpMethod(metadata.Method),new Uri(ValidateGateway(state.Gateway),metadata.Path))){if(metadata.Chunked||metadata.Length>0)outgoing.Content=new LocalContent(input,metadata,cancelled.Token);AddHeaders(outgoing,metadata.Headers,state);
          using(var response=await client.SendAsync(outgoing,HttpCompletionOption.ResponseHeadersRead,cancelled.Token))using(var responseCancellation=cancelled.Token.Register(()=>{try{response.Dispose();}catch{}})){
#if HARBOR_CAPACITY_OBSERVATION
            capacityDelivery=CapacityDeliveryObservation.Begin(metadata.Headers,response.Content.Headers.ContentType==null?null:response.Content.Headers.ContentType.MediaType);
#endif
            var header=new StringBuilder("HTTP/1.1 "+(int)response.StatusCode+" Response\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n");foreach(var item in response.Headers.Concat(response.Content.Headers)){if(Hop.Contains(item.Key)||item.Key.Equals("Content-Length",StringComparison.OrdinalIgnoreCase))continue;var value=String.Join(", ",item.Value);if(!value.Contains("\r")&&!value.Contains("\n"))header.Append(item.Key+": "+value+"\r\n");}header.Append("\r\n");var encoded=Encoding.ASCII.GetBytes(header.ToString());await network.WriteAsync(encoded,0,encoded.Length,cancelled.Token);started=true;
            using(var source=await response.Content.ReadAsStreamAsync()){var bytes=new byte[65536];for(;;){int count=await source.ReadAsync(bytes,0,bytes.Length,cancelled.Token);if(count==0)break;var prefix=Encoding.ASCII.GetBytes(count.ToString("x")+"\r\n");await network.WriteAsync(prefix,0,prefix.Length,cancelled.Token);await network.WriteAsync(bytes,0,count,cancelled.Token);await network.WriteAsync(new byte[]{13,10},0,2,cancelled.Token);await network.FlushAsync(cancelled.Token);
#if HARBOR_CAPACITY_OBSERVATION
              if(capacityDelivery!=null)capacityDelivery.Written(bytes,count,System.Diagnostics.Stopwatch.GetTimestamp());
#endif
            }}await network.WriteAsync(Encoding.ASCII.GetBytes("0\r\n\r\n"),0,5,cancelled.Token);
#if HARBOR_CAPACITY_OBSERVATION
            capacityComplete=true;
#endif
          }
        }
      }catch(DeviceError error){localError=error.Message;localStatus=error.Message=="device_request_too_large"?413:400;}catch{localError="device_transport_unavailable";}finally{
#if HARBOR_CAPACITY_OBSERVATION
        if(capacityDelivery!=null)capacityDelivery.End(System.Diagnostics.Stopwatch.GetTimestamp(),capacityComplete);else CapacityDeliveryObservation.NoProbeEnd(capacityTrace,System.Diagnostics.Stopwatch.GetTimestamp());
#endif
        finished.TrySetResult(true);cancelled.Cancel();}if(!started&&localError!=null)try{await ErrorResponse(network,localStatus,localError);}catch{}await monitor;
    }
  }
  sealed class ClientLease : IDisposable {
    public readonly State State;public readonly HttpClient Client;readonly Action release;public ClientLease(State state,HttpClient client,Action done){State=state;Client=client;release=done;}public void Dispose(){release();}
  }
  sealed class DeviceClients : IDisposable {
    sealed class Entry {public State State;public HttpClient Client;public int Users;public bool Retired;}
    readonly string path,deviceId;readonly object gate=new object();Entry current;bool closed;
    public DeviceClients(string file,State initial){path=file;deviceId=initial.DeviceId;Capture(initial);current=new Entry{State=initial,Client=Client(initial,true)};}
    public ClientLease Acquire(){lock(gate){if(closed)throw new DeviceError("device_transport_stopping");var next=Load(path);if(next.DeviceId!=deviceId||next.KeyName!=current.State.KeyName)throw new DeviceError("device_running_identity_changed_restart_required");if(next.CertificateThumbprint!=current.State.CertificateThumbprint){Capture(next);var replacement=new Entry{State=next,Client=Client(next,true)};current.Retired=true;if(current.Users==0)current.Client.Dispose();current=replacement;}var entry=current;entry.Users++;return new ClientLease(entry.State,entry.Client,()=>{lock(gate){entry.Users--;if(entry.Retired&&entry.Users==0)entry.Client.Dispose();}});}}
    public void Dispose(){lock(gate){closed=true;current.Retired=true;if(current.Users==0)current.Client.Dispose();}}
  }
  static async Task HandleDeviceTcp(TcpClient local,DeviceClients clients,string localToken,int headerTimeout,CancellationToken stopping){try{using(var lease=clients.Acquire())await HandleTcp(local,lease.Client,lease.State,localToken,headerTimeout,stopping);}catch{local.Close();}}
  static async Task Relay(string[] args){
    var state=Load(StatePath(args));using(var client=new TcpClient()){await client.ConnectAsync(IPAddress.Loopback,state.Port);var input=new BufferedInput(Console.OpenStandardInput());var metadata=await ReadLocal(input);metadata.Headers.Remove("X-Harbor-Local-Token");metadata.Headers["X-Harbor-Local-Token"]=Unprotect(state.LocalTokenProtected);metadata.Headers["Host"]="127.0.0.1:"+state.Port;metadata.Headers["Connection"]="close";var header=new StringBuilder(metadata.Method+" "+metadata.Path+" HTTP/1.1\r\n");foreach(var pair in metadata.Headers)header.Append(pair.Key+": "+pair.Value+"\r\n");header.Append("\r\n");var network=client.GetStream();var bytes=Encoding.ASCII.GetBytes(header.ToString());await network.WriteAsync(bytes,0,bytes.Length);var sending=input.CopyToAsync(network);try{await network.CopyToAsync(Console.OpenStandardOutput());}finally{client.Close();}sending.ContinueWith(done=>{var ignored=done.Exception;},TaskContinuationOptions.OnlyOnFaulted);}
  }
  static async Task Serve(string[] args) {
    var state=Load(StatePath(args));int headerTimeout=int.Parse(Option(args,"--header-timeout-ms","15000")),maxConcurrent=int.Parse(Option(args,"--max-concurrent","32"));if(maxConcurrent<1||maxConcurrent>128)throw new DeviceError("device_concurrency_invalid");if(headerTimeout<1000||headerTimeout>30000)throw new DeviceError("device_header_timeout_invalid");var localToken=Unprotect(state.LocalTokenProtected);var listener=new TcpListener(IPAddress.Loopback,state.Port);using(var clients=new DeviceClients(StatePath(args),state))using(var cancel=new CancellationTokenSource()){
      listener.Start(32);var stopping=new TaskCompletionSource<bool>();var tasks=new HashSet<Task>();var gate=new object();
#if HARBOR_CAPACITY_OBSERVATION
      if(Array.IndexOf(args,"--capacity-control-stdin")>=0)CapacityDeliveryObservation.StartControl(()=>stopping.TrySetResult(true));
#endif
      Console.CancelKeyPress+=(sender,e)=>{e.Cancel=true;stopping.TrySetResult(true);};Console.WriteLine(Json.Serialize(new {state="running",local_port=state.Port,key_protection=state.KeyProtection,hardware=state.FingerprintSource}));
      Exception failure=null;try{for(;;){var next=listener.AcceptTcpClientAsync();if(await Task.WhenAny(next,stopping.Task)!=next)break;var context=await next;lock(gate){if(tasks.Count>=maxConcurrent){context.Close();continue;}}
          var task=HandleDeviceTcp(context,clients,localToken,headerTimeout,cancel.Token);lock(gate)tasks.Add(task);task.ContinueWith(done=>{lock(gate)tasks.Remove(done);});
        }
      }catch(Exception error){failure=error;}listener.Stop();Task[] active;lock(gate)active=tasks.ToArray();var joined=Task.WhenAll(active);if(await Task.WhenAny(joined,Task.Delay(300000))!=joined)cancel.Cancel();try{await joined;}catch{}listener.Stop();if(failure!=null)throw failure;
    }
  }
  static void Diagnose(string[] args) { var state=Load(StatePath(args));bool key=false,certificate=false;try{using(var rsa=OpenKey(state))key=rsa.Key.ExportPolicy==CngExportPolicies.None;}catch{}try{using(var cert=Certificate(state))certificate=cert.HasPrivateKey;}catch{}Console.WriteLine(Json.Serialize(new {state=certificate&&key?"ready":"requires_attention",key_available=key,certificate_available=certificate,key_protection=state.KeyProtection,hardware=state.FingerprintSource})); }
  static void Remove(string[] args) { var path=StatePath(args);var state=Load(path);var thumbs=state.CertificateThumbprints??new List<string>();if(!String.IsNullOrEmpty(state.CertificateThumbprint)&&!thumbs.Contains(state.CertificateThumbprint))thumbs.Add(state.CertificateThumbprint);using(var store=new X509Store(StoreName.My,StoreLocation.CurrentUser)){store.Open(OpenFlags.ReadWrite);foreach(var thumb in thumbs.Distinct())foreach(var cert in store.Certificates.Find(X509FindType.FindByThumbprint,thumb,false))store.Remove(cert);}var provider=new CngProvider(state.KeyProvider);if(CngKey.Exists(state.KeyName,provider))using(var key=CngKey.Open(state.KeyName,provider))key.Delete();File.Delete(path);Console.WriteLine("{\"state\":\"removed_locally_server_policy_unchanged\"}"); }
  public static int Main(string[] args) { try{
#if HARBOR_CAPACITY_OBSERVATION
    CapacityObservationEnabled=Array.IndexOf(args,"--capacity-observe-http")>=0;
    CapacityDeliveryObservation.Configure(Array.IndexOf(args,"--capacity-observe-delivery")>=0);
#endif
    if(args.Length==0)throw new DeviceError("device_command_required");switch(args[0]){case "prepare":Prepare(args).GetAwaiter().GetResult();break;case "complete":Complete(args).GetAwaiter().GetResult();break;case "renew":Renew(args).GetAwaiter().GetResult();break;case "serve":Serve(args).GetAwaiter().GetResult();break;case "relay":Relay(args).GetAwaiter().GetResult();break;case "diagnose":Diagnose(args);break;case "local-token":Console.Write(Unprotect(Load(StatePath(args)).LocalTokenProtected));break;case "remove":Remove(args);break;default:throw new DeviceError("device_command_invalid");}return 0;}catch(DeviceError error){Console.Error.WriteLine(error.Message);return 1;}catch(Exception error){var nested=error.InnerException as WebException;Console.Error.WriteLine("device_operation_failed_"+Stage+"_"+error.GetType().Name+(error.InnerException==null?"":"_"+error.InnerException.GetType().Name)+(nested==null?"":"_"+nested.Status));return 1;}
#if HARBOR_CAPACITY_OBSERVATION
    finally{CapacityDeliveryObservation.Complete();}
#endif
  }
}
