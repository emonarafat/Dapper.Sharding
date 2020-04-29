using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace Dapper.Sharding
{
	internal class ObjectReader : DbDataReader
	{
		private IEnumerator source;

		private readonly TypeAccessor accessor;

		private readonly string[] memberNames;

		private readonly Type[] effectiveTypes;

		private readonly BitArray allowNull;

		private object current;

		private bool active = true;

		public override int Depth => 0;

		public override bool HasRows => active;

		public override int RecordsAffected => 0;

		public override int FieldCount => memberNames.Length;

		public override bool IsClosed => source == null;

		public override object this[string name] => accessor[current, name] ?? DBNull.Value;

		public override object this[int i] => accessor[current, memberNames[i]] ?? DBNull.Value;

		public static ObjectReader Create<T>(IEnumerable<T> source, params string[] members)
		{
			return new ObjectReader(typeof(T), source, members);
		}

		public ObjectReader(Type type, IEnumerable source, params string[] members)
		{
			if (source == null)
			{
				throw new ArgumentOutOfRangeException("source");
			}
			bool flag = members == null || members.Length == 0;
			accessor = TypeAccessor.Create(type);
			if (accessor.GetMembersSupported)
			{
				MemberSet members2 = accessor.GetMembers();
				if (flag)
				{
					members = new string[members2.Count];
					for (int i = 0; i < members.Length; i++)
					{
						members[i] = members2[i].Name;
					}
				}
				allowNull = new BitArray(members.Length);
				effectiveTypes = new Type[members.Length];
				for (int j = 0; j < members.Length; j++)
				{
					Type type2 = null;
					bool value = true;
					string b = members[j];
					foreach (Member item in members2)
					{
						if (item.Name == b)
						{
							if (!(type2 == null))
							{
								type2 = null;
								break;
							}
							Type type3 = item.Type;
							type2 = (Nullable.GetUnderlyingType(type3) ?? type3);
							value = (!TypeHelpers._IsValueType(type2) || !(type2 == type3));
						}
					}
					allowNull[j] = value;
					effectiveTypes[j] = (type2 ?? typeof(object));
				}
			}
			else if (flag)
			{
				throw new InvalidOperationException("Member information is not available for this type; the required members must be specified explicitly");
			}
			current = null;
			memberNames = (string[])members.Clone();
			this.source = source.GetEnumerator();
		}

		public override DataTable GetSchemaTable()
		{
			DataTable dataTable = new DataTable
			{
				Columns = 
				{
					{
						"ColumnOrdinal",
						typeof(int)
					},
					{
						"ColumnName",
						typeof(string)
					},
					{
						"DataType",
						typeof(Type)
					},
					{
						"ColumnSize",
						typeof(int)
					},
					{
						"AllowDBNull",
						typeof(bool)
					}
				}
			};
			object[] array = new object[5];
			for (int i = 0; i < memberNames.Length; i++)
			{
				array[0] = i;
				array[1] = memberNames[i];
				array[2] = ((effectiveTypes == null) ? typeof(object) : effectiveTypes[i]);
				array[3] = -1;
				array[4] = (allowNull == null || allowNull[i]);
				dataTable.Rows.Add(array);
			}
			return dataTable;
		}

		public override void Close()
		{
			Shutdown();
		}

		public override bool NextResult()
		{
			active = false;
			return false;
		}

		public override bool Read()
		{
			if (active)
			{
				IEnumerator enumerator = source;
				if (enumerator != null && enumerator.MoveNext())
				{
					current = enumerator.Current;
					return true;
				}
				active = false;
			}
			current = null;
			return false;
		}

		protected override void Dispose(bool disposing)
		{
			base.Dispose(disposing);
			if (disposing)
			{
				Shutdown();
			}
		}

		private void Shutdown()
		{
			active = false;
			current = null;
			IDisposable disposable = source as IDisposable;
			source = null;
			disposable?.Dispose();
		}

		public override bool GetBoolean(int i)
		{
			return (bool)this[i];
		}

		public override byte GetByte(int i)
		{
			return (byte)this[i];
		}

		public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
		{
			byte[] array = (byte[])this[i];
			int num = array.Length - (int)fieldOffset;
			if (num <= 0)
			{
				return 0L;
			}
			int num2 = TypeHelpers.Min(length, num);
			Buffer.BlockCopy(array, (int)fieldOffset, buffer, bufferoffset, num2);
			return num2;
		}

		public override char GetChar(int i)
		{
			return (char)this[i];
		}

		public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
		{
			string text = (string)this[i];
			int num = text.Length - (int)fieldoffset;
			if (num <= 0)
			{
				return 0L;
			}
			int num2 = TypeHelpers.Min(length, num);
			text.CopyTo((int)fieldoffset, buffer, bufferoffset, num2);
			return num2;
		}

		protected override DbDataReader GetDbDataReader(int i)
		{
			throw new NotSupportedException();
		}

		public override string GetDataTypeName(int i)
		{
			return ((effectiveTypes == null) ? typeof(object) : effectiveTypes[i]).Name;
		}

		public override DateTime GetDateTime(int i)
		{
			return (DateTime)this[i];
		}

		public override decimal GetDecimal(int i)
		{
			return (decimal)this[i];
		}

		public override double GetDouble(int i)
		{
			return (double)this[i];
		}

		public override Type GetFieldType(int i)
		{
			if (effectiveTypes != null)
			{
				return effectiveTypes[i];
			}
			return typeof(object);
		}

		public override float GetFloat(int i)
		{
			return (float)this[i];
		}

		public override Guid GetGuid(int i)
		{
			return (Guid)this[i];
		}

		public override short GetInt16(int i)
		{
			return (short)this[i];
		}

		public override int GetInt32(int i)
		{
			return (int)this[i];
		}

		public override long GetInt64(int i)
		{
			return (long)this[i];
		}

		public override string GetName(int i)
		{
			return memberNames[i];
		}

		public override int GetOrdinal(string name)
		{
			return Array.IndexOf(memberNames, name);
		}

		public override string GetString(int i)
		{
			return (string)this[i];
		}

		public override object GetValue(int i)
		{
			return this[i];
		}

		public override IEnumerator GetEnumerator()
		{
			return new DbEnumerator((IDataReader)this);
		}

		public override int GetValues(object[] values)
		{
			string[] array = memberNames;
			object target = current;
			TypeAccessor typeAccessor = accessor;
			int num = TypeHelpers.Min(values.Length, array.Length);
			for (int i = 0; i < num; i++)
			{
				values[i] = (typeAccessor[target, array[i]] ?? DBNull.Value);
			}
			return num;
		}

		public override bool IsDBNull(int i)
		{
			return this[i] is DBNull;
		}
	}
}
