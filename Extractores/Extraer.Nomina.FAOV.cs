// INFORMACIÓN
//
// Nombre:
//      Extraccion de Datos para el fondo de ahorro obligatorio para la vivienda 
// Descripción:
//      Archivo de código para la extracción de los datos necesarios para 
//      la generacion del archivo de texto plano para el fondo de ahorro 
//      obligatorio para la vivienda.
// Empresa:
//      Softech Sistemas
// Autor:
//      Softech Sistemas
// Fecha:
//      20 de Abril de 2.009
// Actualización: 07/08/2018
//      Cambio en el diccionario del Extractor para el manejo correcto
//      de los tipos de datos DateTime para evitar el conflicto con la 
//      cultura del Sistema Operativo.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.IO;

namespace Softech.Nomina.Extraccion
{
    /// <summary>
    /// Clase para extraer datos del sistema
    /// </summary>
    public static class FAOV
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
                query.Append("SELECT  E.cod_emp, Case when E.nac=1 then 'V' when E.nac=2 then 'E' when E.nac=3 then 'P' end as nac, E.ci, ltrim(rtrim(E.nombres)) as nombres, ");
                query.Append("ltrim(rtrim(E.apellidos)) as apellidos, sum(N.monto) as SALARIO_INT, ");
 		query.Append("[dbo].[ObtenerValorCampoAdicional] ('CFAOV') AS cuentabanco,");
                query.Append("E.fecha_ing, E.fecha_egr ");
                query.Append("FROM SNEMPLE E ");
                query.Append("LEFT JOIN SNNOMI N ON N.COD_EMP = E.COD_EMP and N.co_conce = 'Q023' AND (N.fec_emis >= @f_fecha_i) AND (N.fec_emis <= @f_fecha_f) ");
                query.Append("INNER JOIN snem_va V ON V.cod_emp = E.cod_emp and V.co_var = 'R003' and (V.val_c <> 'N' or V.val_c is null) ");
                query.Append("WHERE ");
                query.Append("(exists (select ni.reci_num from snnomi ni where NI.COD_EMP = E.COD_EMP and Ni.co_conce = 'R003' AND (Ni.fec_emis >= @f_fecha_i) AND (Ni.fec_emis <= @f_fecha_f))) and ");
                query.Append("(@f_emple_i IS NULL OR (E.cod_emp >= @f_emple_i)) AND ");
                query.Append("(@f_emple_f IS NULL OR (E.cod_emp <= @f_emple_f)) AND ");
                query.Append("(@f_depart_i IS NULL OR (E.co_depart >= @f_depart_i)) AND ");
                query.Append("(@f_depart_f IS NULL OR (E.co_depart <= @f_depart_f)) AND ");
                query.Append("(@f_cont_i IS NULL OR (E.co_cont >= @f_cont_i)) AND ");
                query.Append("(@f_cont_f IS NULL OR (E.co_cont <= @f_cont_f)) AND ");
                query.Append("(@f_ubicacion_i IS NULL OR (E.co_ubicacion >= @f_ubicacion_i)) AND ");
                query.Append("(@f_ubicacion_f IS NULL OR (E.co_ubicacion <= @f_ubicacion_f)) AND ");
                query.Append("(@f_cargo_i IS NULL OR (E.co_cargo >= @f_cargo_i)) AND ");
                query.Append("(@f_cargo_f IS NULL OR (E.co_cargo <= @f_cargo_f)) ");
                query.Append("group by E.cod_emp,E.nombres, E.apellidos, E.ci, E.nac,  E.fecha_ing, E.fecha_egr ");
                query.Append("having sum(N.monto) is not null");
                SqlCommand comando = new SqlCommand(query.ToString(), conexion);

                #endregion

                #region Crear la coleccion de parametros

                #region Instanciar los parametros

                //comando.Parameters.Add(new SqlParameter("@prueba", SqlDbType.Char, 1));
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

                //comando.Parameters["@prueba"].SqlValue = filtros["sprueba"];

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
                da.Fill(extraccionDataSet, "faov");

                #endregion
            }

            return extraccionDataSet;
        }
    }
}