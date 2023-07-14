// INFORMACIÓN
//
// Nombre:
//      Extraccion de Datos de la nómina para el banco Banesco
// Descripción:
//      Archivo de código para la extracción de los datos necesarios para 
//      la generacion de los datos de la nómina para el archivo de texto 
//      plano para el banco Banesco.
// Empresa:
//      Softech Sistemas
// Autor:
//      Softech Sistemas
// Fecha:
//      17 de Septiembre de 2.008
// Actualización: 07/08/2018
//      Cambio en el diccionario del Extractor para el manejo correcto
//      de los tipos de datos DateTime para evitar el conflicto con la 
//      cultura del Sistema Operativo.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace Softech.Nomina.Extraccion
{
    /// <summary>
    /// Clase para extraer datos del sistema
    /// </summary>
    public static class BNCNO2
    {
        /// <summary>
        /// Extrae un DataSet con la informacion necesaria para el proceso de extracción de datos
        /// </summary>
        /// <param name="conexion">Objeto de conexión sql con la conexion abierta</param>
        /// <param name="filtros">Diccionario con los filtros y valores para la extracción de datos</param>
        /// <param name="parametros">Parametros necesarios para la extracción de datos</param>
        /// <returns>Set de datos con la informacion de la extracción</returns>
        public static Object Extraer(SqlConnection conexion,
            Dictionary<String, Object> filtros,
            Dictionary<String, Object> parametros,
            String masterProfit)
        {
            DataSet extraccionDataSet = new DataSet("Extraccion");

            #region Comprobar el estado de la conexion

            if (conexion.State != ConnectionState.Open)
                throw new ArgumentException("Para poder extraer los datos es necesario que la conexion esté abierta.");

            #endregion

            using (conexion)
            {
                #region Armar query

                StringBuilder query = new StringBuilder();
                query.Append("declare @rif_emp varchar(16), @nom_emp varchar(40); ");
                query.Append("select @nom_emp = desc_empresa, @rif_emp = rif ");
                query.Append("from ");
                query.Append(masterProfit.Trim());
                query.Append(".dbo.MpEmpresa ");
                query.Append("where Cod_Empresa = @db ");
                query.Append("select @nom_emp nom_emp, @rif_emp rif_emp, ");
                query.Append("isnull(e.cta_banc1, '') num_cta, e.nac, e.ci, ");
                query.Append("e.nombres, e.apellidos, isnull(e.direccion, '') direccion, e.correo_e, ");
		query.Append("[dbo].[ObtenerValorCampoAdicional] ('CTABAN') AS cuentabanco,");
                query.Append("isnull(e.telefono, '') telefono, isnull(e.avisar_a, '') avisar_a, ");
                query.Append("sum(CASE n.tipo WHEN 1 THEN n.monto ELSE 0 END) - ");
                query.Append("sum(CASE n.tipo WHEN 2 THEN n.monto ELSE 0 END) - ");
                query.Append("sum(CASE n.tipo WHEN 3 THEN n.monto ELSE 0 END) monto, r.reci_num ");
                query.Append("from dbo.snemple e ");
                query.Append("inner join dbo.snrecibo r ");
                query.Append("on r.cod_emp = e.cod_emp ");
                query.Append("inner join dbo.snnomi n ");
                query.Append("on n.reci_num = r.reci_num ");
              //  query.Append("where (e.co_ban1 = @cod_ban) AND ");
                query.Append("where (@f_emple_i IS NULL OR (e.cod_emp >= @f_emple_i)) AND ");
                query.Append("(@f_emple_f IS NULL OR (e.cod_emp <= @f_emple_f)) AND ");
                query.Append("(@f_depart_i IS NULL OR (r.co_depart >= @f_depart_i)) AND ");
                query.Append("(@f_depart_f IS NULL OR (r.co_depart <= @f_depart_f)) AND ");
                query.Append("(@f_cont_i IS NULL OR (r.co_cont >= @f_cont_i)) AND ");
                query.Append("(@f_cont_f IS NULL OR (r.co_cont <= @f_cont_f)) AND ");
                query.Append("(@f_ubicacion_i IS NULL OR (e.co_ubicacion >= @f_ubicacion_i)) AND ");
                query.Append("(@f_ubicacion_f IS NULL OR (e.co_ubicacion <= @f_ubicacion_f)) AND ");
                query.Append("(@f_cargo_i IS NULL OR (e.co_cargo >= @f_cargo_i)) AND ");
                query.Append("(@f_cargo_f IS NULL OR (e.co_cargo <= @f_cargo_f)) AND ");
                query.Append("(r.fec_emis >= @f_fecha_i) AND ");
                query.Append("(r.fec_emis <= @f_fecha_f) AND ");
                query.Append("e.cta_banc1 is not null ");
                query.Append("group by e.cod_emp, r.reci_num, e.cta_banc1, e.nac, e.ci, e.nombres, e.apellidos, e.correo_e, ");
                query.Append("e.direccion, e.telefono, e.avisar_a ");
                query.Append("having (sum(CASE n.tipo WHEN 1 THEN n.monto ELSE 0 END) - sum(CASE n.tipo WHEN 2 THEN n.monto ELSE 0 END) - sum(CASE n.tipo WHEN 3 THEN n.monto ELSE 0 END) > 0)");

                SqlCommand comando = new SqlCommand(query.ToString(), conexion);

                #endregion

                #region Crear la coleccion de parametros

                #region Instanciar los parametros

                comando.Parameters.Add(new SqlParameter("@db", SqlDbType.Char, 20));
             //   comando.Parameters.Add(new SqlParameter("@cod_ban", SqlDbType.Char, 8));
                comando.Parameters.Add(new SqlParameter("@f_fecha_i", SqlDbType.SmallDateTime));
                comando.Parameters.Add(new SqlParameter("@f_fecha_f", SqlDbType.SmallDateTime));
                comando.Parameters.Add(new SqlParameter("@f_emple_i", SqlDbType.Char, 17));
                comando.Parameters.Add(new SqlParameter("@f_emple_f", SqlDbType.Char, 17));
                comando.Parameters.Add(new SqlParameter("@f_depart_i", SqlDbType.Char, 12));
                comando.Parameters.Add(new SqlParameter("@f_depart_f", SqlDbType.Char, 12));
                comando.Parameters.Add(new SqlParameter("@f_cont_i", SqlDbType.Char, 12));
                comando.Parameters.Add(new SqlParameter("@f_cont_f", SqlDbType.Char, 12));
                comando.Parameters.Add(new SqlParameter("@f_ubicacion_i", SqlDbType.Char, 8));
                comando.Parameters.Add(new SqlParameter("@f_ubicacion_f", SqlDbType.Char, 8));
                comando.Parameters.Add(new SqlParameter("@f_cargo_i", SqlDbType.Char, 8));
                comando.Parameters.Add(new SqlParameter("@f_cargo_f", SqlDbType.Char, 8));

                comando.Parameters["@f_emple_i"].IsNullable = true;
                comando.Parameters["@f_emple_i"].SqlValue = DBNull.Value;

                comando.Parameters["@f_emple_f"].IsNullable = true;
                comando.Parameters["@f_emple_f"].SqlValue = DBNull.Value;

                comando.Parameters["@f_depart_i"].IsNullable = true;
                comando.Parameters["@f_depart_i"].SqlValue = DBNull.Value;

                comando.Parameters["@f_depart_f"].IsNullable = true;
                comando.Parameters["@f_depart_f"].SqlValue = DBNull.Value;

                comando.Parameters["@f_cont_i"].IsNullable = true;
                comando.Parameters["@f_cont_i"].SqlValue = DBNull.Value;

                comando.Parameters["@f_cont_f"].IsNullable = true;
                comando.Parameters["@f_cont_f"].SqlValue = DBNull.Value;

                comando.Parameters["@f_ubicacion_i"].IsNullable = true;
                comando.Parameters["@f_ubicacion_i"].SqlValue = DBNull.Value;

                comando.Parameters["@f_ubicacion_f"].IsNullable = true;
                comando.Parameters["@f_ubicacion_f"].SqlValue = DBNull.Value;

                comando.Parameters["@f_cargo_i"].IsNullable = true;
                comando.Parameters["@f_cargo_i"].SqlValue = DBNull.Value;

                comando.Parameters["@f_cargo_f"].IsNullable = true;
                comando.Parameters["@f_cargo_f"].SqlValue = DBNull.Value;

                #endregion

                #region Cargar los valores a los parametros

                comando.Parameters["@db"].SqlValue = conexion.Database.Trim();
              //  comando.Parameters["@cod_ban"].SqlValue = (String)filtros["scoban"];

                if (filtros.ContainsKey("fechaDesde"))
                    comando.Parameters["@f_fecha_i"].SqlValue = (DateTime)filtros["fechaDesde"];

                if (filtros.ContainsKey("fechaHasta"))
                    comando.Parameters["@f_fecha_f"].SqlValue = (DateTime)filtros["fechaHasta"];

                if (filtros.ContainsKey("trabajadorDesde") && !String.IsNullOrEmpty((String)filtros["trabajadorDesde"]))
                    comando.Parameters["@f_emple_i"].SqlValue = filtros["trabajadorDesde"];

                if (filtros.ContainsKey("trabajadorHasta") && !String.IsNullOrEmpty((String)filtros["trabajadorHasta"]))
                    comando.Parameters["@f_emple_f"].SqlValue = filtros["trabajadorHasta"];

                if (filtros.ContainsKey("departamentoDesde") && !String.IsNullOrEmpty((String)filtros["departamentoDesde"]))
                    comando.Parameters["@f_depart_i"].SqlValue = filtros["departamentoDesde"];

                if (filtros.ContainsKey("departamentoHasta") && !String.IsNullOrEmpty((String)filtros["departamentoHasta"]))
                    comando.Parameters["@f_depart_f"].SqlValue = filtros["departamentoHasta"];

                if (filtros.ContainsKey("contratoDesde") && !String.IsNullOrEmpty((String)filtros["contratoDesde"]))
                    comando.Parameters["@f_cont_i"].SqlValue = filtros["contratoDesde"];

                if (filtros.ContainsKey("contratoHasta") && !String.IsNullOrEmpty((String)filtros["contratoHasta"]))
                    comando.Parameters["@f_cont_f"].SqlValue = filtros["contratoHasta"];

                if (filtros.ContainsKey("ubicacionDesde") && !String.IsNullOrEmpty((String)filtros["ubicacionDesde"]))
                    comando.Parameters["@f_ubicacion_i"].SqlValue = filtros["ubicacionDesde"];

                if (filtros.ContainsKey("ubicacionHasta") && !String.IsNullOrEmpty((String)filtros["ubicacionHasta"]))
                    comando.Parameters["@f_ubicacion_f"].SqlValue = filtros["ubicacionHasta"];

                if (filtros.ContainsKey("cargoDesde") && !String.IsNullOrEmpty((String)filtros["cargoDesde"]))
                    comando.Parameters["@f_cargo_i"].SqlValue = filtros["cargoDesde"];

                if (filtros.ContainsKey("cargoHasta") && !String.IsNullOrEmpty((String)filtros["cargoHasta"]))
                    comando.Parameters["@f_cargo_f"].SqlValue = filtros["cargoHasta"];

                #endregion

                #endregion

                #region Ejecutar query

                SqlDataAdapter da = new SqlDataAdapter(comando);
                da.Fill(extraccionDataSet, "Nomina");

                #endregion
            }

            return extraccionDataSet;
        }
    }
}
