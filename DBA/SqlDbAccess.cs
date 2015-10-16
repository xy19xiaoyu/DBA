//类名：SqlDbAccess 
//功能：SQL SERVER 专用的链接数据库的操作类
//作者：陈晓雨
//编写时间：2007-6-30

using System.Collections;
using System.Data.Sql;
using System.Data.SqlClient;
using System.Configuration;
using System.Data;
using System;
/// <summary>
/// SQL SERVER数据库操作类
/// </summary>
/// <remarks>
/// string strSQL = "UPDATE [TABLENAME] SET [COLNAME]=@VALUE WHERE ID=@ID"
/// dim params(1) = new  SqlParameter()
/// params(0) = SqlParameter("@VALUE",sqldbtype.varchar,10)
/// params(1) = sqlparameter("@id",sqldbtype.int)
/// params(0).value  = "234"
/// parmas(1).value = 123
/// try
///     if sqldbaccess.ExecNoQuery(commandtype.text,strsql,params) != 0 then
///     //TODO: something
///     end if
/// catch ex as Exception
/// end try
/// 
/// </remarks>
public class SqlDbAccess
{

    #region "字段"
    private static string _Connstr = System.Configuration.ConfigurationManager.ConnectionStrings["SqlServerStr"].ToString().Trim();


    private static Hashtable parmCache = Hashtable.Synchronized(new Hashtable());
    #endregion

    public static string Connstr
    {
        get { return SqlDbAccess._Connstr; }
        set { SqlDbAccess._Connstr = value; }
    }
    #region "方法"

    /// <summary>
    /// 得到默认的数据库链接对象
    /// </summary>
    /// <return s></return s>
    /// <remarks>
    /// e.g.:dim conn as SqlConnection = SqlDbAccess.GetSqlConnection()
    /// 得到一个ODBC数据库链接对象 链接字符串为Web.Config文件中的ConnectionStrings("SqlServerConnString")节点
    /// </remarks>
    public static SqlConnection GetSqlConnection()
    {
        return new SqlConnection(_Connstr);
    }
    /// <summary>
    /// 得到链接字符串的数据库链接对象没啥用
    /// </summary>
    /// <remarks>
    /// e.g.: dim conn as new  SqlConnection = SqlDbAccess.GetSqlConnection("链接字符串...")
    ///       dim conn as new  SqlConnection("链接字符串...")
    /// 两者一样的
    /// </remarks>      
    /// <param name="ConnString">一个合法的链接字符串</param>
    /// <return s></return s>
    public static SqlConnection GetSqlConnection(string ConnString)
    {
        return new SqlConnection(ConnString);
    }

    /// <summary>
    /// 使用默认的数据库链接 执行没有返回结果集的查询  
    /// </summary>
    /// <param name="cmdType">查询类型T-SQL语句\存储过程</param>
    /// <param name="cmdText">查询语句</param>
    /// <param name="params">查询参数数组</param>
    /// <return s>返回所影响的行数</return s>
    /// <remarks></remarks>
    public static int ExecNoQuery(CommandType cmdType, string cmdText, params SqlParameter[] param)
    {
        SqlCommand cmd = new SqlCommand();
        using (SqlConnection conn = new SqlConnection(Connstr))
        {
            PreparativeCommand(cmd, conn, null, cmdType, cmdText, param);
            int val = cmd.ExecuteNonQuery();
            cmd.Parameters.Clear();
            return val;
        }
    }
    /// <summary>
    /// 使用指定的数据库链接字符串 执行没有返回结果集的查询 
    /// </summary>
    /// <param name="ConnString">数据库链接字符串</param>
    /// <param name="cmdType">查询类型T-SQL语句\存储过程</param>
    /// <param name="cmdText">查询语句</param>
    /// <param name="params">查询参数数组</param>
    /// <return s>返回所影响的行数</return s>
    /// <remarks></remarks>
    public static int ExecNoQuery(string ConnString, CommandType cmdType, string cmdText, params SqlParameter[] param)
    {
        SqlCommand cmd = new SqlCommand();
        using (SqlConnection con = new SqlConnection(ConnString))
        {
            PreparativeCommand(cmd, con, null, cmdType, cmdText, param);
            int val = cmd.ExecuteNonQuery();
            cmd.Parameters.Clear();
            return val;
        }
    }
    /// <summary>
    /// 使用指定的数据库链接 执行没有返回结果集的查询 
    /// </summary>
    /// <param name="Conn">已存在的数据库链接对象</param>
    /// <param name="cmdType">查询类型T-SQL语句\存储过程</param>
    /// <param name="cmdText">查询语句</param>
    /// <param name="params">查询参数数组</param>
    /// <return s>返回所影响的行数</return s>
    /// <remarks></remarks>
    public static int ExecNoQuery(SqlConnection conn, CommandType cmdType, string cmdText, params SqlParameter[] parms)
    {
        SqlCommand cmd = new SqlCommand();
        int val = 0;
        PreparativeCommand(cmd, conn, null, cmdType, cmdText, parms); ;
        val = cmd.ExecuteNonQuery();
        cmd.Parameters.Clear();
        return val;
    }
    /// <summary>
    /// 使用指定的数据库链接事务 执行没有返回结果集的查询  
    /// </summary>
    /// <param name="trans">已经存在的数据库事务</param>
    /// <param name="cmdType">查询类型T-SQL语句\存储过程</param>
    /// <param name="cmdText">查询语句</param>
    /// <param name="params">查询参数数组</param>
    /// <return s>返回所影响的行数</return s>
    /// <remarks></remarks>
    public static int ExecNoQuery(SqlTransaction trans, CommandType cmdType, string cmdText, params SqlParameter[] parms)
    {
        SqlCommand cmd = new SqlCommand();
        PreparativeCommand(cmd, trans.Connection, trans, cmdType, cmdText, parms);
        int val = cmd.ExecuteNonQuery();
        cmd.Parameters.Clear();
        return val;
    }

    /// <summary>
    /// 查询 使用默认的数据库链接 返回数据流
    /// </summary>
    /// <param name="cmdType">查询类型T-SQL语句\存储过程</param>
    /// <param name="cmdText">查询语句</param>
    /// <param name="params">查询参数</param>
    /// <return s>数据流</return s>
    /// <remarks></remarks>
    public static SqlDataReader ExecuteReader(CommandType cmdType, string cmdText, params SqlParameter[] parms)
    {
        SqlCommand cmd = new SqlCommand();
        SqlDataReader odbcReader;
        SqlConnection conn = new SqlConnection(Connstr);
        PreparativeCommand(cmd, conn, null, cmdType, cmdText, parms);
        odbcReader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
        cmd.Parameters.Clear();
        return odbcReader;
    }
    /// <summary>
    /// 查询 使用指定的数据库链接字符串 返回数据流
    /// </summary>
    /// <param name="ConnString">一个合法的链接字符串</param>
    /// <param name="cmdType">查询类型T-SQL语句\存储过程</param>
    /// <param name="cmdText">查询语句</param>
    /// <param name="params">查询参数</param>
    /// <return s>数据流</return s>
    /// <remarks></remarks>
    public static SqlDataReader ExecuteReader(string ConnString, CommandType cmdType, string cmdText, params SqlParameter[] parms)
    {
        SqlCommand cmd = new SqlCommand();
        SqlDataReader odbcReader;
        SqlConnection conn = new SqlConnection(ConnString);
        PreparativeCommand(cmd, conn, null, cmdType, cmdText, parms);
        odbcReader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
        cmd.Parameters.Clear();
        return odbcReader;
    }
    /// <summary>
    /// 查询 使用指定的数据库链接 返回数据流
    /// </summary>
    /// <param name="conn">已存在的数据库链接对象</param>
    /// <param name="cmdType">查询类型T-SQL语句\存储过程</param>
    /// <param name="cmdText">查询语句</param>
    /// <param name="params">查询参数</param>
    /// <return s>数据流</return s>
    /// <remarks></remarks>
    public static SqlDataReader ExecuteReader(SqlConnection conn, CommandType cmdType, string cmdText, params SqlParameter[] parms)
    {
        SqlCommand cmd = new SqlCommand();
        SqlDataReader odbcReader;
        PreparativeCommand(cmd, conn, null, cmdType, cmdText, parms);
        odbcReader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
        cmd.Parameters.Clear();
        return odbcReader;
    }
    /// <summary>
    /// 查询 使用指定的数据库链接事务 返回数据流
    /// </summary>
    /// <param name="trans">已经存在的数据库事务</param>
    /// <param name="cmdType">查询类型T-SQL语句\存储过程</param>
    /// <param name="cmdText">查询语句</param>
    /// <param name="params">查询参数</param>
    /// <return s>数据流</return s>
    /// <remarks></remarks>
    public static SqlDataReader ExecuteReader(SqlTransaction trans, CommandType cmdType, string cmdText, params SqlParameter[] parms)
    {

        SqlCommand cmd = new SqlCommand();
        SqlDataReader odbcReader;
        PreparativeCommand(cmd, null, trans, cmdType, cmdText, parms);
        odbcReader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
        cmd.Parameters.Clear();
        return odbcReader;
    }



    /// <summary>
    /// 查询数据 使用默认的数据库链接 返回第一行的第一列
    /// </summary>
    /// <param name="cmdType">查询类型T-SQL语句\存储过程</param>
    /// <param name="cmdText">查询语句</param>
    /// <param name="params">查询参数</param>
    /// <return s></return s>
    /// <remarks></remarks>
    public static object ExecuteScalar(CommandType cmdType, string cmdText, params SqlParameter[] parms)
    {
        SqlCommand cmd = new SqlCommand();
        using (SqlConnection conn = new SqlConnection(Connstr))
        {
            PreparativeCommand(cmd, conn, null, cmdType, cmdText, parms);
            object val = cmd.ExecuteScalar();
            cmd.Parameters.Clear();
            return val;
        }

    }
    /// <summary>
    /// 查询数据 使用指定的连接字符串 返回第一行的第一列
    /// </summary>
    /// <param name="ConnString">使用指定的连接字符串</param>
    /// <param name="cmdType">查询类型T-SQL语句\存储过程</param>
    /// <param name="cmdText">查询语句</param>
    /// <param name="params">查询参数</param>
    /// <return s></return s>
    /// <remarks></remarks>
    public static object ExecuteScalar(string ConnString, CommandType cmdType, string cmdText, params SqlParameter[] parms)
    {
        SqlCommand cmd = new SqlCommand();
        using (SqlConnection conn = new SqlConnection(ConnString))
        {
            PreparativeCommand(cmd, conn, null, cmdType, cmdText, parms);
            object val = cmd.ExecuteScalar();
            cmd.Parameters.Clear();
            return val;
        }

    }
    /// <summary>
    /// 查询数据 使用指定的数据库链接 返回第一行的第一列
    /// </summary>
    /// <param name="conn">一个已经开启的数据库连接</param>
    /// <param name="cmdType">查询类型T-SQL语句\存储过程</param>
    /// <param name="cmdText">查询语句</param>
    /// <param name="params">查询参数</param>
    /// <return s></return s>
    /// <remarks></remarks>
    public static object ExecuteScalar(SqlConnection conn, CommandType cmdType, string cmdText, params SqlParameter[] parms)
    {
        SqlCommand cmd = new SqlCommand();

        PreparativeCommand(cmd, conn, null, cmdType, cmdText, parms);
        object val = cmd.ExecuteScalar();
        cmd.Parameters.Clear();
        return val;

    }
    /// <summary>
    /// 查询数据 使用指定的数据库链接事务 返回第一行的第一列
    /// </summary>
    /// <param name="trans">一个已经开启的事务</param>
    /// <param name="cmdType">查询类型T-SQL语句\存储过程</param>
    /// <param name="cmdText">查询语句</param>
    /// <param name="params">查询参数</param>
    /// <return s></return s>
    /// <remarks></remarks>
    public static object ExecuteScalar(SqlTransaction trans, CommandType cmdType, string cmdText, params SqlParameter[] parms)
    {
        SqlCommand cmd = new SqlCommand();
        PreparativeCommand(cmd, trans.Connection, trans, cmdType, cmdText, parms);
        object val = cmd.ExecuteScalar();
        cmd.Parameters.Clear();
        return val;

    }


    /// <summary>
    /// 查询数据 使用默认的数据库链接   返回查询结果集
    /// </summary>
    /// <param name="cmdType">查询类型T-SQL语句\存储过程</param>
    /// <param name="cmdText">查询语句</param>
    /// <param name="params">查询参数</param>
    /// <return s>DataSet</return s>
    /// <remarks></remarks>
    public static DataSet GetDataSet(CommandType cmdType, string cmdText, params SqlParameter[] parms)
    {
        SqlCommand cmd = new SqlCommand();
        DataSet ds = new DataSet();
        using (SqlConnection conn = new SqlConnection(Connstr))
        {
            PreparativeCommand(cmd, conn, null, cmdType, cmdText, parms);
            SqlDataAdapter adapter = new SqlDataAdapter(cmd);
            adapter.Fill(ds);
            cmd.Parameters.Clear();
            return ds;
        }
    }
    /// <summary>
    /// 查询数据 使用指定的连接字符串   返回查询结果集
    /// </summary>
    /// <param name="ConnString">一个合法的链接字符串</param>
    /// <param name="cmdType">查询类型T-SQL语句\存储过程</param>
    /// <param name="cmdText">查询语句</param>
    /// <param name="params">查询参数</param>
    /// <return s>DataSet</return s>
    /// <remarks></remarks>
    public static DataSet GetDataSet(string ConnString, CommandType cmdType, string cmdText, params SqlParameter[] parms)
    {
        SqlCommand cmd = new SqlCommand();
        DataSet ds = new DataSet();
        using (SqlConnection conn = new SqlConnection(ConnString))
        {
            PreparativeCommand(cmd, conn, null, cmdType, cmdText, parms);
            SqlDataAdapter adapter = new SqlDataAdapter(cmd);
            adapter.Fill(ds);
            cmd.Parameters.Clear();
            return ds;
        }
    }
    /// <summary>
    /// 查询数据 使用指定的数据库链接   返回查询结果集
    /// </summary>
    /// <param name="cmdType">查询类型T-SQL语句\存储过程</param>
    /// <param name="cmdText">查询语句</param>
    /// <param name="params">查询参数</param>
    /// <return s>DataSet</return s>
    /// <remarks></remarks>
    public static DataSet GetDataSet(SqlConnection conn, CommandType cmdType, string cmdText, params SqlParameter[] parms)
    {
        SqlCommand cmd = new SqlCommand();
        DataSet ds = new DataSet();
        PreparativeCommand(cmd, conn, null, cmdType, cmdText, parms);
        SqlDataAdapter adapter = new SqlDataAdapter(cmd);
        adapter.Fill(ds);
        cmd.Parameters.Clear();
        return ds;
    }
    /// <summary>
    /// 查询数据 使用的指定数据库链接事务 返回查询结果集
    /// </summary>
    /// <param name="trans">指定数据库链接事务</param>
    /// <param name="cmdType">查询类型T-SQL语句\存储过程</param>
    /// <param name="cmdText">查询语句</param>
    /// <param name="params">查询参数</param>
    /// <return s>DataSet</return s>
    /// <remarks></remarks>
    public static DataSet GetDataSet(SqlTransaction trans, CommandType cmdType, string cmdText, params SqlParameter[] parms)
    {
        SqlCommand cmd = new SqlCommand();
        DataSet ds = new DataSet();
        PreparativeCommand(cmd, trans.Connection, trans, cmdType, cmdText, parms);
        SqlDataAdapter adapter = new SqlDataAdapter(cmd);
        adapter.Fill(ds);
        cmd.Parameters.Clear();
        return ds;
    }

    /// <summary>
    /// 查询数据 使用默认的数据库链接 返回查询结果集
    /// </summary>
    /// <param name="cmdType">查询类型T-SQL语句\存储过程</param>
    /// <param name="cmdText">查询语句</param>
    /// <param name="params">查询参数</param>
    /// <return s>DataTable</return s>
    /// <remarks></remarks>
    public static DataTable GetDataTable(CommandType cmdType, string cmdText, params SqlParameter[] parms)
    {
        SqlCommand cmd = new SqlCommand();
        DataTable table = new DataTable();
        using (SqlConnection conn = new SqlConnection(Connstr))
        {
            PreparativeCommand(cmd, conn, null, cmdType, cmdText, parms);
            SqlDataAdapter adapter = new SqlDataAdapter(cmd);
            adapter.Fill(table);
            cmd.Parameters.Clear();
            return table;
        }
    }
    /// <summary>
    /// 查询数据 使用指定的连接字符串   返回查询结果集
    /// </summary>
    /// <param name="ConnString">一个合法的链接字符串</param>
    /// <param name="cmdType">查询类型T-SQL语句\存储过程</param>
    /// <param name="cmdText">查询语句</param>
    /// <param name="params">查询参数</param>
    /// <return s>DataTable</return s>
    /// <remarks></remarks>
    public static DataTable GetDataTable(string ConnString, CommandType cmdType, string cmdText, params SqlParameter[] parms)
    {
        SqlCommand cmd = new SqlCommand();
        DataTable table = new DataTable();
        using (SqlConnection conn = new SqlConnection(ConnString))
        {
            PreparativeCommand(cmd, conn, null, cmdType, cmdText, parms);
            SqlDataAdapter adapter = new SqlDataAdapter(cmd);
            adapter.Fill(table);
            cmd.Parameters.Clear();
            return table;
        }
    }
    /// <summary>
    /// 查询数据 使用指定的数据库链接   返回查询结果集
    /// </summary>
    /// <param name="conn">一个打开的数据库连接</param>
    /// <param name="cmdType">查询类型T-SQL语句\存储过程</param>
    /// <param name="cmdText">查询语句</param>
    /// <param name="params">查询参数</param>
    /// <return s>DataTable</return s>
    /// <remarks></remarks>
    public static DataTable GetDataTable(SqlConnection conn, CommandType cmdType, string cmdText, params SqlParameter[] parms)
    {
        SqlCommand cmd = new SqlCommand();
        DataTable table = new DataTable();
        PreparativeCommand(cmd, conn, null, cmdType, cmdText, parms);
        SqlDataAdapter adapter = new SqlDataAdapter(cmd);
        adapter.Fill(table);
        cmd.Parameters.Clear();
        return table;
    }
    /// <summary>
    /// 查询数据 使用的指定数据库链接事务 返回查询结果集
    /// </summary>
    /// <param name="trans">一个已经开启的事务</param>
    /// <param name="cmdType">查询类型T-SQL语句\存储过程</param>
    /// <param name="cmdText">查询语句</param>
    /// <param name="params">查询参数</param>
    /// <return s>DataTable</return s>
    /// <remarks></remarks>
    public static DataTable GetDataTable(SqlTransaction trans, CommandType cmdType, string cmdText, params SqlParameter[] parms)
    {
        SqlCommand cmd = new SqlCommand();
        DataTable table = new DataTable();
        PreparativeCommand(cmd, trans.Connection, trans, cmdType, cmdText, parms);
        SqlDataAdapter adapter = new SqlDataAdapter(cmd);
        adapter.Fill(table);
        cmd.Parameters.Clear();
        return table;
    }


    /// <summary>
    /// 为执行T-SQL语句做准备
    /// </summary>
    /// <param name="cmd">SqlCommand 对象</param>
    /// <param name="conn">数据库链接对象</param>
    /// <param name="trans">数据库操作事务</param>
    /// <param name="cmdType">查询类型</param>
    /// <param name="cmdText">查询语句</param>
    /// <param name="parms">查询需要的参数数组</param>
    /// <remarks></remarks>
    private static void PreparativeCommand(SqlCommand cmd, SqlConnection conn, SqlTransaction trans, CommandType cmdType, string cmdText, params SqlParameter[] parms)
    {

        //如果链接没有打开 打开链接
        if (conn.State != ConnectionState.Open)
        {
            conn.Open();
        }

        //设置数据库链接
        cmd.Connection = conn;
        //查询语句
        cmd.CommandText = cmdText;
        //查询类型  T-SQL \ 存储过程
        cmd.CommandType = cmdType;

        //是使用事务提交还是不使用
        if (trans != null)
        {
            cmd.Transaction = trans;
        }

        //如果查询参数不为空(给参数赋值)
        if (parms != null)
        {
            foreach (SqlParameter par in parms)
            {
                cmd.Parameters.Add(par);
            }
        }
    }
    /// <summary>
    /// 缓存查询参数
    /// </summary>
    /// <param name="cacheKey">名字</param>
    /// <param name="parms">参数数组</param>
    /// <remarks>
    /// 保存需要缓存的参数数组
    /// </remarks>
    public void staticCacheParameters(string cacheKey, SqlParameter[] parms)
    {
        parmCache[cacheKey] = parms;
    }
    /// <summary>
    /// 得到缓存的参数
    /// </summary>
    /// <param name="cacheKey">名字</param>
    /// <return s></return s>
    /// <remarks></remarks>
    public static SqlParameter[] GetCachedParameters(string cacheKey)
    {

        //判断是否保存了需要的参数
        SqlParameter[] parms = parmCache[cacheKey] as SqlParameter[];
        //如果没有就返回null
        if (parms == null)
        {
            return null;
        }
        SqlParameter[] clonedParms = new SqlParameter[parms.Length];
        //赋值
        for (int i = 0; i < parms.Length; i++)
        {
            clonedParms[1] = ((ICloneable)parms[i]).Clone() as SqlParameter;
        }
        return clonedParms;
    }
    #endregion

}