using System;
using System.Dynamic;

namespace Dapper.Sharding
{
	internal abstract class ObjectAccessor
	{
		private sealed class TypeAccessorWrapper : ObjectAccessor
		{
			private readonly object target;

			private readonly TypeAccessor accessor;

			public override object this[string name]
			{
				get
				{
					return accessor[target, name];
				}
				set
				{
					accessor[target, name] = value;
				}
			}

			public override object Target => target;

			public TypeAccessorWrapper(object target, TypeAccessor accessor)
			{
				this.target = target;
				this.accessor = accessor;
			}
		}

		private sealed class DynamicWrapper : ObjectAccessor
		{
			private readonly IDynamicMetaObjectProvider target;

			public override object Target => target;

			public override object this[string name]
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

			public DynamicWrapper(IDynamicMetaObjectProvider target)
			{
				this.target = target;
			}
		}

		public abstract object this[string name]
		{
			get;
			set;
		}

		public abstract object Target
		{
			get;
		}

		public override bool Equals(object obj)
		{
			return Target.Equals(obj);
		}

		public override int GetHashCode()
		{
			return Target.GetHashCode();
		}

		public override string ToString()
		{
			return Target.ToString();
		}

		public static ObjectAccessor Create(object target)
		{
			return Create(target, allowNonPublicAccessors: false);
		}

		public static ObjectAccessor Create(object target, bool allowNonPublicAccessors)
		{
			if (target == null)
			{
				throw new ArgumentNullException("target");
			}
			IDynamicMetaObjectProvider dynamicMetaObjectProvider = target as IDynamicMetaObjectProvider;
			if (dynamicMetaObjectProvider != null)
			{
				return new DynamicWrapper(dynamicMetaObjectProvider);
			}
			return new TypeAccessorWrapper(target, TypeAccessor.Create(target.GetType(), allowNonPublicAccessors));
		}
	}
}
