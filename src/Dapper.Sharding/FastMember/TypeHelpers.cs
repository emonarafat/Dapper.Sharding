using System;
using System.Reflection.Emit;

namespace Dapper.Sharding
{
	internal static class TypeHelpers
	{
		public static readonly Type[] EmptyTypes = Type.EmptyTypes;

		public static bool _IsValueType(Type type)
		{
			return type.IsValueType;
		}

		public static bool _IsPublic(Type type)
		{
			return type.IsPublic;
		}

		public static bool _IsNestedPublic(Type type)
		{
			return type.IsNestedPublic;
		}

		public static bool _IsClass(Type type)
		{
			return type.IsClass;
		}

		public static bool _IsAbstract(Type type)
		{
			return type.IsAbstract;
		}

		public static Type _CreateType(TypeBuilder type)
		{
			return type.CreateType();
		}

		public static int Min(int x, int y)
		{
			if (x >= y)
			{
				return y;
			}
			return x;
		}
	}
}
