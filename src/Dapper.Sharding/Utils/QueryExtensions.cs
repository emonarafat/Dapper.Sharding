using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Dapper.Sharding
{
    internal static class QueryExtensions
    {
        public static IQueryable<T> OrderBy<T>(this IQueryable<T> source, string propertyName, bool asc = true)
        {
            ParameterExpression parameter = Expression.Parameter(source.ElementType, String.Empty);
            MemberExpression property = Expression.Property(parameter, propertyName);
            LambdaExpression lambda = Expression.Lambda(property, parameter);

            string methodName = asc ? "OrderBy" : "OrderByDescending";

            Expression methodCallExpression = Expression.Call(typeof(Queryable), methodName,
                                                new Type[] { source.ElementType, property.Type },
                                                source.Expression, Expression.Quote(lambda));

            return source.Provider.CreateQuery<T>(methodCallExpression);

        }

        public static IEnumerable<TEntity> OrderBy<TEntity>(this IEnumerable<TEntity> source, string orderByProperty, bool asc = true)
        {
            string command = asc ? "OrderBy" : "OrderByDescending";
            var type = typeof(TEntity);
            var property = type.GetProperty(orderByProperty);
            var parameter = Expression.Parameter(type, "p");
            var propertyAccess = Expression.MakeMemberAccess(parameter, property);
            var orderByExpression = Expression.Lambda(propertyAccess, parameter);
            var resultExpression = Expression.Call(typeof(Queryable), command,
                                                   new[] { type, property.PropertyType },
                                                   source.AsQueryable().Expression,
                                                   Expression.Quote(orderByExpression));
            return source.AsQueryable().Provider.CreateQuery<TEntity>(resultExpression);
        }

    }

}
