using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Dapper.Sharding
{
	internal sealed class MemberSet : IEnumerable<Member>, IEnumerable, IList<Member>, ICollection<Member>
	{
		private Member[] members;

		public Member this[int index] => members[index];

		public int Count => members.Length;

		Member IList<Member>.this[int index]
		{
			get
			{
				return members[index];
			}
			set
			{
				throw new NotSupportedException();
			}
		}

		bool ICollection<Member>.IsReadOnly => true;

		internal MemberSet(Type type)
		{
			members = (from x in type.GetProperties(BindingFlags.Instance | BindingFlags.Public).Cast<MemberInfo>().Concat(type.GetFields(BindingFlags.Instance | BindingFlags.Public).Cast<MemberInfo>())
				orderby x.Name
				select x into member
				select new Member(member)).ToArray();
		}

		public IEnumerator<Member> GetEnumerator()
		{
			Member[] array = members;
			for (int i = 0; i < array.Length; i++)
			{
				yield return array[i];
			}
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		bool ICollection<Member>.Remove(Member item)
		{
			throw new NotSupportedException();
		}

		void ICollection<Member>.Add(Member item)
		{
			throw new NotSupportedException();
		}

		void ICollection<Member>.Clear()
		{
			throw new NotSupportedException();
		}

		void IList<Member>.RemoveAt(int index)
		{
			throw new NotSupportedException();
		}

		void IList<Member>.Insert(int index, Member item)
		{
			throw new NotSupportedException();
		}

		bool ICollection<Member>.Contains(Member item)
		{
			return members.Contains(item);
		}

		void ICollection<Member>.CopyTo(Member[] array, int arrayIndex)
		{
			members.CopyTo(array, arrayIndex);
		}

		int IList<Member>.IndexOf(Member member)
		{
			return Array.IndexOf(members, member);
		}
	}
}
