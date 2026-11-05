#if HARBOR_CAPACITY_OBSERVATION
// Test-build instrumentation only. A completed socket write is observable;
// it does not prove that the client application has consumed the content.
using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Diagnostics;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Globalization;
using System.Security.Cryptography;
using System.Web.Script.Serialization;

internal static class CapacityDeliveryObservation {
  const int MaxEventBytes = 1048576, MaxItems = 2048;
  static readonly string Epoch = Guid.NewGuid().ToString();
  static readonly UTF8Encoding StrictUtf8 = new UTF8Encoding(false,true);
  static readonly System.Text.RegularExpressions.Regex FrameBoundary = new System.Text.RegularExpressions.Regex("\r?\n\r?\n",System.Text.RegularExpressions.RegexOptions.CultureInvariant);
  static readonly BlockingCollection<string> Queue = new BlockingCollection<string>(128);
  static readonly object Gate = new object();
  static Thread writer;
  static bool enabled, failed;
  static long sequence, dropped;
  static JavaScriptSerializer Serializer() { return new JavaScriptSerializer { MaxJsonLength = MaxEventBytes, RecursionLimit = 64 }; }

  public static void Configure(bool value) {
    if(!value)return;
    lock(Gate) {
      if(enabled)return;
      enabled=true;
      writer=new Thread(()=>{
        bool inFlight=false;
        try { foreach(var line in Queue.GetConsumingEnumerable()){inFlight=true;Console.WriteLine(line);inFlight=false;} }
        catch {
          lock(Gate){failed=true;if(inFlight)dropped++;string lost;while(Queue.TryTake(out lost))dropped++;}
          // Stop publishing after an I/O failure, but keep the terminal snapshot
          // tied to the real producer completion rather than an early fault.
          foreach(var lost in Queue.GetConsumingEnumerable())lock(Gate)dropped++;
        }
        try { string status;lock(Gate)status=Serializer().Serialize(new {capacity_delivery="helper-socket-write-complete-v1",type="status",clockEpoch=Epoch,sequence=sequence.ToString(CultureInfo.InvariantCulture),dropped,fault=failed,formalQualified=false});Console.WriteLine(status); }catch{lock(Gate){failed=true;dropped++;}}
      });
      writer.IsBackground=true;
      writer.Start();
    }
  }
  static void Emit(Dictionary<string,object> fields) {
    lock(Gate) {
      if(!enabled)return;
      fields["capacity_delivery"]="helper-socket-write-complete-v1";
      fields["sequence"]=(++sequence).ToString(CultureInfo.InvariantCulture);
      fields["clockEpoch"]=Epoch;
      fields["frequency"]=Stopwatch.Frequency.ToString(CultureInfo.InvariantCulture);
      fields["dropped"]=dropped;
      fields["fault"]=failed;
      fields["formalQualified"]=false;
      try { if(failed||!Queue.TryAdd(Serializer().Serialize(fields))) { dropped++;failed=true; } }
      catch { dropped++;failed=true; }
    }
  }
  public static void Complete() {
    lock(Gate) { if(!enabled)return;Queue.CompleteAdding(); }
    if(writer!=null)writer.Join(1000);
  }
  public static object Status() { lock(Gate)return new {queued=Queue.Count,maxPending=128,dropped,fault=failed,formalQualified=false}; }
  public static void StartControl(Action stop) {
    if(!enabled)return;
    System.Threading.Tasks.Task.Run(()=>{
      var line=new StringBuilder();bool oversized=false;
      try { for(;;) { int next=Console.In.Read();if(next<0)return;if(next=='\n') {
        long received=Stopwatch.GetTimestamp();string command=line.ToString().TrimEnd('\r');
        if(!oversized&&command=="capacity-stop") { stop();return; }
        Guid request;
        if(!oversized&&command.StartsWith("capacity-clock ",StringComparison.Ordinal)&&Guid.TryParseExact(command.Substring(15),"D",out request))
          Emit(new Dictionary<string,object>{{"type","clock"},{"requestId",request.ToString()},{"receivedTicks",received.ToString(CultureInfo.InvariantCulture)},{"sentTicks",Stopwatch.GetTimestamp().ToString(CultureInfo.InvariantCulture)}});
        line.Clear();oversized=false;
      }else if(line.Length<64)line.Append((char)next);else oversized=true; } }
      catch { lock(Gate)failed=true; }
    });
  }
  public static Probe Begin(IDictionary<string,string> headers,string contentType) {
    string trace;Guid parsed;
    if(!enabled||!String.Equals(contentType,"text/event-stream",StringComparison.OrdinalIgnoreCase)||!headers.TryGetValue("X-Capacity-Trace",out trace)||!Guid.TryParseExact(trace,"D",out parsed))return null;
    return new Probe(trace);
  }
  public static string AuthenticatedTrace(IDictionary<string,string> headers) {
    string value;Guid trace;return enabled&&headers.TryGetValue("X-Capacity-Trace",out value)&&Guid.TryParseExact(value,"D",out trace)?value:null;
  }
  public static void NoProbeEnd(string trace,long ticks) {
    if(trace!=null)Emit(new Dictionary<string,object>{{"trace",trace},{"type","end"},{"atTicks",ticks.ToString(CultureInfo.InvariantCulture)},{"complete",false},{"probeCreated",false}});
  }
  public sealed class Probe {
    readonly string trace;
    readonly Decoder decoder = StrictUtf8.GetDecoder();
    readonly StringBuilder pending = new StringBuilder();
    readonly JavaScriptSerializer json = Serializer();
    long ordinal;
    bool invalid,firstCharacter=true;
    internal Probe(string value) { trace=value; }
    void Invalid(long ticks) {
      if(invalid)return;invalid=true;pending.Clear();
      Emit(new Dictionary<string,object>{{"trace",trace},{"type","unavailable"},{"atTicks",ticks.ToString(CultureInfo.InvariantCulture)}});
    }
    public void Written(byte[] bytes,int count,long ticks) {
      if(invalid)return;
      try {
        if(count<0||count>bytes.Length||count>65536)throw new InvalidDataException();
        var chars=new char[StrictUtf8.GetMaxCharCount(count)];int used=decoder.GetChars(bytes,0,count,chars,0,false);
        int begin=0;if(firstCharacter&&used>0){firstCharacter=false;if(chars[0]=='\ufeff')begin=1;}pending.Append(chars,begin,used-begin);
        for(;;) {
          var text=pending.ToString();var separator=FrameBoundary.Match(text);if(!separator.Success)break;int at=separator.Index;
          if(at>MaxEventBytes)throw new InvalidDataException();int length=separator.Length;
          var frame=text.Substring(0,at);pending.Remove(0,at+length);
          var data=new StringBuilder();string eventType=null;
          foreach(var line in frame.Replace("\r\n","\n").Split('\n')) {
            if(line.StartsWith("data:",StringComparison.Ordinal)) { if(data.Length>0)data.Append('\n');data.Append(line.Substring(5).TrimStart(' ','\t')); }
            else if(line.StartsWith("event:",StringComparison.Ordinal))eventType=line.Substring(6).Trim();
          }
          if(data.Length==0||data.ToString()=="[DONE]")continue;
          var value=json.DeserializeObject(data.ToString()) as Dictionary<string,object>;if(value==null)throw new InvalidDataException();
          if(!value.ContainsKey("type")&&eventType!=null)value["type"]=eventType;
          var identity=Classify(value);if(identity==null)continue;
          identity["trace"]=trace;identity["type"]="content";identity["ordinal"]=ordinal++;identity["atTicks"]=ticks.ToString(CultureInfo.InvariantCulture);Emit(identity);
        }
        if(pending.Length>MaxEventBytes)throw new InvalidDataException();
      } catch { Invalid(ticks); }
    }
    public void End(long ticks,bool complete) {
      if(!invalid)try { var tail=new char[2];if(decoder.GetChars(new byte[0],0,0,tail,0,true)>0||pending.ToString().Trim().Length>0)Invalid(ticks); }catch{Invalid(ticks);}
      Emit(new Dictionary<string,object>{{"trace",trace},{"type","end"},{"atTicks",ticks.ToString(CultureInfo.InvariantCulture)},{"complete",complete&&!invalid},{"probeCreated",true}});
    }
  }
  static string Text(IDictionary<string,object> value,string key) { object item;return value.TryGetValue(key,out item)?item as string:null; }
  static Dictionary<string,object> Classify(Dictionary<string,object> value) {
    var type=Text(value,"type");var bytes=new List<byte>();string kind;int count=0;
    if(type=="response.output_text.delta"||type=="response.function_call_arguments.delta") {
      var delta=Text(value,"delta");if(String.IsNullOrEmpty(delta))return null;
      var id=Text(value,"item_id")??"";if(id.Length>200)throw new InvalidDataException();
      object index;string output=value.TryGetValue("output_index",out index)&&index!=null?Convert.ToString(index,CultureInfo.InvariantCulture):"";
      bytes.AddRange(Encoding.UTF8.GetBytes(type+"\0"+id+"\0"+output+"\0"));bytes.AddRange(Encoding.UTF8.GetBytes(delta));count=Encoding.UTF8.GetByteCount(delta);kind=type=="response.output_text.delta"?"text":"arguments";
    }else if(type=="response.completed") {
      object responseValue;if(!value.TryGetValue("response",out responseValue))return null;var response=responseValue as Dictionary<string,object>;
      if(response==null||Text(response,"status")!="completed")return null;
      object outputValue;if(!response.TryGetValue("output",out outputValue))return null;var output=outputValue as object[];if(output==null)return null;if(output.Length>MaxItems)throw new InvalidDataException();int items=0;
      foreach(var raw in output) {
        var item=raw as Dictionary<string,object>;if(item==null)continue;var parts=new List<string>();
        if(Text(item,"type")=="message") {
          object contents;var content=item.TryGetValue("content",out contents)?contents as object[]:null;if(content!=null) { if(content.Length>MaxItems-items)throw new InvalidDataException();items+=content.Length;foreach(var part in content) { var detail=part as Dictionary<string,object>;var text=detail==null?null:Text(detail,"text");if(!String.IsNullOrEmpty(text))parts.Add(text); } }
        }else if(Text(item,"type")=="function_call"&&!String.IsNullOrEmpty(Text(item,"name"))) { items+=2;parts.Add(Text(item,"name"));parts.Add(Text(item,"arguments")??""); }
        foreach(var text in parts) { var encoded=Encoding.UTF8.GetBytes(text);count+=encoded.Length;if(items>MaxItems||count>MaxEventBytes)throw new InvalidDataException();bytes.AddRange(Encoding.UTF8.GetBytes(encoded.Length.ToString(CultureInfo.InvariantCulture)+":"));bytes.AddRange(encoded); }
      }
      if(count==0)return null;kind="terminal";
    }else return null;
    if(count>MaxEventBytes)throw new InvalidDataException();
    string hash;using(var sha=SHA256.Create())hash=BitConverter.ToString(sha.ComputeHash(bytes.ToArray())).Replace("-","").ToLowerInvariant();
    return new Dictionary<string,object>{{"contentKind",kind},{"hash",hash},{"bytes",count}};
  }
}
#endif
