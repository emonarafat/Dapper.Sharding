using Microsoft.CSharp.RuntimeBinder;
using System;
using System.Collections;
using System.Runtime.CompilerServices;

namespace Dapper.Sharding
{
	internal static class CallSiteCache
	{
		private static readonly Hashtable getters = new Hashtable();

		private static readonly Hashtable setters = new Hashtable();

		internal static object GetValue(string name, object target)
		{
			CallSite<Func<CallSite, object, object>> callSite = (CallSite<Func<CallSite, object, object>>)getters[name];
			if (callSite == null)
			{
				CallSite<Func<CallSite, object, object>> callSite2 = CallSite<Func<CallSite, object, object>>.Create(Binder.GetMember(CSharpBinderFlags.None, name, typeof(CallSiteCache), new CSharpArgumentInfo[1]
				{
					CSharpArgumentInfo.Create(CSharpArgumentInfoFlags.None, null)
				}));
				lock (getters)
				{
					callSite = (CallSite<Func<CallSite, object, object>>)getters[name];
					if (callSite == null)
					{
						callSite = (CallSite<Func<CallSite, object, object>>)(getters[name] = callSite2);
					}
				}
			}
			return callSite.Target(callSite, target);
		}

		internal static void SetValue(string name, object target, object value)
		{
			CallSite<Func<CallSite, object, object, object>> callSite = (CallSite<Func<CallSite, object, object, object>>)setters[name];
			if (callSite == null)
			{
				CallSite<Func<CallSite, object, object, object>> callSite2 = CallSite<Func<CallSite, object, object, object>>.Create(Binder.SetMember(CSharpBinderFlags.None, name, typeof(CallSiteCache), new CSharpArgumentInfo[2]
				{
					CSharpArgumentInfo.Create(CSharpArgumentInfoFlags.None, null),
					CSharpArgumentInfo.Create(CSharpArgumentInfoFlags.UseCompileTimeType, null)
				}));
				lock (setters)
				{
					callSite = (CallSite<Func<CallSite, object, object, object>>)setters[name];
					if (callSite == null)
					{
						callSite = (CallSite<Func<CallSite, object, object, object>>)(setters[name] = callSite2);
					}
				}
			}
			callSite.Target(callSite, target, value);
		}
	}
}
