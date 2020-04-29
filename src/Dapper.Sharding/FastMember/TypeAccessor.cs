using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;

namespace Dapper.Sharding
{
	internal abstract class TypeAccessor
	{
		private sealed class DynamicAccessor : TypeAccessor
		{
			public static readonly DynamicAccessor Singleton = new DynamicAccessor();

			public override object this[object target, string name]
			{
				get
				{
					return CallSiteCache.GetValue(name, target);
				}
				set
				{
					CallSiteCache.SetValue(name, target, value);
				}
			}

			private DynamicAccessor()
			{
			}
		}

		protected abstract class RuntimeTypeAccessor : TypeAccessor
		{
			private MemberSet members;

			protected abstract Type Type
			{
				get;
			}

			public override bool GetMembersSupported => true;

			public override MemberSet GetMembers()
			{
				return members ?? (members = new MemberSet(Type));
			}
		}

		private sealed class DelegateAccessor : RuntimeTypeAccessor
		{
			private readonly Dictionary<string, int> map;

			private readonly Func<int, object, object> getter;

			private readonly Action<int, object, object> setter;

			private readonly Func<object> ctor;

			private readonly Type type;

			protected override Type Type => type;

			public override bool CreateNewSupported => ctor != null;

			public override object this[object target, string name]
			{
				get
				{
					if (map.TryGetValue(name, out int value))
					{
						return getter(value, target);
					}
					throw new ArgumentOutOfRangeException("name");
				}
				set
				{
					if (map.TryGetValue(name, out int value2))
					{
						setter(value2, target, value);
						return;
					}
					throw new ArgumentOutOfRangeException("name");
				}
			}

			public DelegateAccessor(Dictionary<string, int> map, Func<int, object, object> getter, Action<int, object, object> setter, Func<object> ctor, Type type)
			{
				this.map = map;
				this.getter = getter;
				this.setter = setter;
				this.ctor = ctor;
				this.type = type;
			}

			public override object CreateNew()
			{
				if (ctor == null)
				{
					return base.CreateNew();
				}
				return ctor();
			}
		}

		private static readonly Hashtable publicAccessorsOnly = new Hashtable();

		private static readonly Hashtable nonPublicAccessors = new Hashtable();

		private static AssemblyBuilder assembly;

		private static ModuleBuilder module;

		private static int counter;

		private static readonly MethodInfo tryGetValue = typeof(Dictionary<string, int>).GetMethod("TryGetValue");

		private static readonly MethodInfo strinqEquals = typeof(string).GetMethod("op_Equality", new Type[2]
		{
			typeof(string),
			typeof(string)
		});

		public virtual bool CreateNewSupported => false;

		public virtual bool GetMembersSupported => false;

		public abstract object this[object target, string name]
		{
			get;
			set;
		}

		public virtual object CreateNew()
		{
			throw new NotSupportedException();
		}

		public virtual MemberSet GetMembers()
		{
			throw new NotSupportedException();
		}

		public static TypeAccessor Create(Type type)
		{
			return Create(type, allowNonPublicAccessors: false);
		}

		public static TypeAccessor Create(Type type, bool allowNonPublicAccessors)
		{
			if (type == null)
			{
				throw new ArgumentNullException("type");
			}
			Hashtable hashtable = allowNonPublicAccessors ? nonPublicAccessors : publicAccessorsOnly;
			TypeAccessor typeAccessor = (TypeAccessor)hashtable[type];
			if (typeAccessor != null)
			{
				return typeAccessor;
			}
			lock (hashtable)
			{
				typeAccessor = (TypeAccessor)hashtable[type];
				if (typeAccessor != null)
				{
					return typeAccessor;
				}
				return (TypeAccessor)(hashtable[type] = CreateNew(type, allowNonPublicAccessors));
			}
		}

		private static int GetNextCounterValue()
		{
			return Interlocked.Increment(ref counter);
		}

		private static void WriteMapImpl(ILGenerator il, Type type, List<MemberInfo> members, FieldBuilder mapField, bool allowNonPublicAccessors, bool isGet)
		{
			Label label = il.DefineLabel();
			OpCode opcode;
			OpCode ldarg_;
			OpCode opcode2;
			if (mapField == null)
			{
				opcode = OpCodes.Ldarg_0;
				ldarg_ = OpCodes.Ldarg_1;
				opcode2 = OpCodes.Ldarg_2;
			}
			else
			{
				il.DeclareLocal(typeof(int));
				opcode = OpCodes.Ldloc_0;
				ldarg_ = OpCodes.Ldarg_1;
				opcode2 = OpCodes.Ldarg_3;
				il.Emit(OpCodes.Ldarg_0);
				il.Emit(OpCodes.Ldfld, mapField);
				il.Emit(OpCodes.Ldarg_2);
				il.Emit(OpCodes.Ldloca_S, (byte)0);
				il.EmitCall(OpCodes.Callvirt, tryGetValue, null);
				il.Emit(OpCodes.Brfalse, label);
			}
			Label[] array = new Label[members.Count];
			for (int i = 0; i < array.Length; i++)
			{
				array[i] = il.DefineLabel();
			}
			il.Emit(opcode);
			il.Emit(OpCodes.Switch, array);
			il.MarkLabel(label);
			il.Emit(OpCodes.Ldstr, "name");
			il.Emit(OpCodes.Newobj, typeof(ArgumentOutOfRangeException).GetConstructor(new Type[1]
			{
				typeof(string)
			}));
			il.Emit(OpCodes.Throw);
			for (int j = 0; j < array.Length; j++)
			{
				il.MarkLabel(array[j]);
				MemberInfo memberInfo = members[j];
				bool flag = true;
				FieldInfo fieldInfo;
				PropertyInfo propertyInfo;
				MethodInfo methodInfo;
				if ((fieldInfo = (memberInfo as FieldInfo)) != null)
				{
					il.Emit(ldarg_);
					Cast(il, type, valueAsPointer: true);
					if (isGet)
					{
						il.Emit(OpCodes.Ldfld, fieldInfo);
						if (TypeHelpers._IsValueType(fieldInfo.FieldType))
						{
							il.Emit(OpCodes.Box, fieldInfo.FieldType);
						}
					}
					else
					{
						il.Emit(opcode2);
						Cast(il, fieldInfo.FieldType, valueAsPointer: false);
						il.Emit(OpCodes.Stfld, fieldInfo);
					}
					il.Emit(OpCodes.Ret);
					flag = false;
				}
				else if ((propertyInfo = (memberInfo as PropertyInfo)) != null && propertyInfo.CanRead && (methodInfo = (isGet ? propertyInfo.GetGetMethod(allowNonPublicAccessors) : propertyInfo.GetSetMethod(allowNonPublicAccessors))) != null)
				{
					il.Emit(ldarg_);
					Cast(il, type, valueAsPointer: true);
					if (isGet)
					{
						il.EmitCall(TypeHelpers._IsValueType(type) ? OpCodes.Call : OpCodes.Callvirt, methodInfo, null);
						if (TypeHelpers._IsValueType(propertyInfo.PropertyType))
						{
							il.Emit(OpCodes.Box, propertyInfo.PropertyType);
						}
					}
					else
					{
						il.Emit(opcode2);
						Cast(il, propertyInfo.PropertyType, valueAsPointer: false);
						il.EmitCall(TypeHelpers._IsValueType(type) ? OpCodes.Call : OpCodes.Callvirt, methodInfo, null);
					}
					il.Emit(OpCodes.Ret);
					flag = false;
				}
				if (flag)
				{
					il.Emit(OpCodes.Br, label);
				}
			}
		}

		private static bool IsFullyPublic(Type type, PropertyInfo[] props, bool allowNonPublicAccessors)
		{
			while (TypeHelpers._IsNestedPublic(type))
			{
				type = type.DeclaringType;
			}
			if (!TypeHelpers._IsPublic(type))
			{
				return false;
			}
			if (allowNonPublicAccessors)
			{
				for (int i = 0; i < props.Length; i++)
				{
					if (props[i].GetGetMethod(nonPublic: true) != null && props[i].GetGetMethod(nonPublic: false) == null)
					{
						return false;
					}
					if (props[i].GetSetMethod(nonPublic: true) != null && props[i].GetSetMethod(nonPublic: false) == null)
					{
						return false;
					}
				}
			}
			return true;
		}

		private static TypeAccessor CreateNew(Type type, bool allowNonPublicAccessors)
		{
			if (typeof(IDynamicMetaObjectProvider).IsAssignableFrom(type))
			{
				return DynamicAccessor.Singleton;
			}
			PropertyInfo[] properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);
			FieldInfo[] fields = type.GetFields(BindingFlags.Instance | BindingFlags.Public);
			Dictionary<string, int> dictionary = new Dictionary<string, int>();
			List<MemberInfo> list = new List<MemberInfo>(properties.Length + fields.Length);
			int num = 0;
			PropertyInfo[] array = properties;
			foreach (PropertyInfo propertyInfo in array)
			{
				if (!dictionary.ContainsKey(propertyInfo.Name) && propertyInfo.GetIndexParameters().Length == 0)
				{
					dictionary.Add(propertyInfo.Name, num++);
					list.Add(propertyInfo);
				}
			}
			FieldInfo[] array2 = fields;
			foreach (FieldInfo fieldInfo in array2)
			{
				if (!dictionary.ContainsKey(fieldInfo.Name))
				{
					dictionary.Add(fieldInfo.Name, num++);
					list.Add(fieldInfo);
				}
			}
			ConstructorInfo constructorInfo = null;
			if (TypeHelpers._IsClass(type) && !TypeHelpers._IsAbstract(type))
			{
				constructorInfo = type.GetConstructor(TypeHelpers.EmptyTypes);
			}
			if (!IsFullyPublic(type, properties, allowNonPublicAccessors))
			{
				DynamicMethod dynamicMethod = new DynamicMethod(type.FullName + "_get", typeof(object), new Type[2]
				{
					typeof(int),
					typeof(object)
				}, type, skipVisibility: true);
				DynamicMethod dynamicMethod2 = new DynamicMethod(type.FullName + "_set", null, new Type[3]
				{
					typeof(int),
					typeof(object),
					typeof(object)
				}, type, skipVisibility: true);
				WriteMapImpl(dynamicMethod.GetILGenerator(), type, list, null, allowNonPublicAccessors, isGet: true);
				WriteMapImpl(dynamicMethod2.GetILGenerator(), type, list, null, allowNonPublicAccessors, isGet: false);
				DynamicMethod dynamicMethod3 = null;
				if (constructorInfo != null)
				{
					dynamicMethod3 = new DynamicMethod(type.FullName + "_ctor", typeof(object), TypeHelpers.EmptyTypes, type, skipVisibility: true);
					ILGenerator iLGenerator = dynamicMethod3.GetILGenerator();
					iLGenerator.Emit(OpCodes.Newobj, constructorInfo);
					iLGenerator.Emit(OpCodes.Ret);
				}
				return new DelegateAccessor(dictionary, (Func<int, object, object>)dynamicMethod.CreateDelegate(typeof(Func<int, object, object>)), (Action<int, object, object>)dynamicMethod2.CreateDelegate(typeof(Action<int, object, object>)), (dynamicMethod3 == null) ? null : ((Func<object>)dynamicMethod3.CreateDelegate(typeof(Func<object>))), type);
			}
			if (assembly == null)
			{
				AssemblyName assemblyName = new AssemblyName("FastMember_dynamic");
				assembly = AppDomain.CurrentDomain.DefineDynamicAssembly(assemblyName, AssemblyBuilderAccess.Run);
				module = assembly.DefineDynamicModule(assemblyName.Name);
			}
			TypeAttributes attributes = typeof(TypeAccessor).Attributes;
			TypeBuilder typeBuilder = module.DefineType("FastMember_dynamic." + type.Name + "_" + GetNextCounterValue(), (attributes | TypeAttributes.Sealed | TypeAttributes.Public) & ~TypeAttributes.Abstract, typeof(RuntimeTypeAccessor));
			ILGenerator iLGenerator2 = typeBuilder.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, new Type[1]
			{
				typeof(Dictionary<string, int>)
			}).GetILGenerator();
			iLGenerator2.Emit(OpCodes.Ldarg_0);
			iLGenerator2.Emit(OpCodes.Ldarg_1);
			FieldBuilder fieldBuilder = typeBuilder.DefineField("_map", typeof(Dictionary<string, int>), FieldAttributes.Private | FieldAttributes.InitOnly);
			iLGenerator2.Emit(OpCodes.Stfld, fieldBuilder);
			iLGenerator2.Emit(OpCodes.Ret);
			PropertyInfo property = typeof(TypeAccessor).GetProperty("Item");
			MethodInfo getMethod = property.GetGetMethod();
			MethodInfo setMethod = property.GetSetMethod();
			MethodBuilder methodBuilder = typeBuilder.DefineMethod(getMethod.Name, getMethod.Attributes & ~MethodAttributes.Abstract, typeof(object), new Type[2]
			{
				typeof(object),
				typeof(string)
			});
			WriteMapImpl(methodBuilder.GetILGenerator(), type, list, fieldBuilder, allowNonPublicAccessors, isGet: true);
			typeBuilder.DefineMethodOverride(methodBuilder, getMethod);
			methodBuilder = typeBuilder.DefineMethod(setMethod.Name, setMethod.Attributes & ~MethodAttributes.Abstract, null, new Type[3]
			{
				typeof(object),
				typeof(string),
				typeof(object)
			});
			WriteMapImpl(methodBuilder.GetILGenerator(), type, list, fieldBuilder, allowNonPublicAccessors, isGet: false);
			typeBuilder.DefineMethodOverride(methodBuilder, setMethod);
			MethodInfo getMethod2;
			if (constructorInfo != null)
			{
				getMethod2 = typeof(TypeAccessor).GetProperty("CreateNewSupported").GetGetMethod();
				methodBuilder = typeBuilder.DefineMethod(getMethod2.Name, getMethod2.Attributes, getMethod2.ReturnType, TypeHelpers.EmptyTypes);
				ILGenerator iLGenerator3 = methodBuilder.GetILGenerator();
				iLGenerator3.Emit(OpCodes.Ldc_I4_1);
				iLGenerator3.Emit(OpCodes.Ret);
				typeBuilder.DefineMethodOverride(methodBuilder, getMethod2);
				getMethod2 = typeof(TypeAccessor).GetMethod("CreateNew");
				methodBuilder = typeBuilder.DefineMethod(getMethod2.Name, getMethod2.Attributes, getMethod2.ReturnType, TypeHelpers.EmptyTypes);
				ILGenerator iLGenerator4 = methodBuilder.GetILGenerator();
				iLGenerator4.Emit(OpCodes.Newobj, constructorInfo);
				iLGenerator4.Emit(OpCodes.Ret);
				typeBuilder.DefineMethodOverride(methodBuilder, getMethod2);
			}
			getMethod2 = typeof(RuntimeTypeAccessor).GetProperty("Type", BindingFlags.Instance | BindingFlags.NonPublic).GetGetMethod(nonPublic: true);
			methodBuilder = typeBuilder.DefineMethod(getMethod2.Name, getMethod2.Attributes & ~MethodAttributes.Abstract, getMethod2.ReturnType, TypeHelpers.EmptyTypes);
			ILGenerator iLGenerator5 = methodBuilder.GetILGenerator();
			iLGenerator5.Emit(OpCodes.Ldtoken, type);
			iLGenerator5.Emit(OpCodes.Call, typeof(Type).GetMethod("GetTypeFromHandle"));
			iLGenerator5.Emit(OpCodes.Ret);
			typeBuilder.DefineMethodOverride(methodBuilder, getMethod2);
			return (TypeAccessor)Activator.CreateInstance(TypeHelpers._CreateType(typeBuilder), dictionary);
		}

		private static void Cast(ILGenerator il, Type type, bool valueAsPointer)
		{
			if (type == typeof(object))
			{
				return;
			}
			if (TypeHelpers._IsValueType(type))
			{
				if (valueAsPointer)
				{
					il.Emit(OpCodes.Unbox, type);
				}
				else
				{
					il.Emit(OpCodes.Unbox_Any, type);
				}
			}
			else
			{
				il.Emit(OpCodes.Castclass, type);
			}
		}
	}
}
