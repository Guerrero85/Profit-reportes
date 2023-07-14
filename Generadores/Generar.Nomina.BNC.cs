// INFORMACIÓN
//
// Nombre:
//      Generador de Datos para la nomina para el banco Banesco
// Descripción:
//      Archivo de código para la generacion de los datos para
//      la nomina para el banco Banesco.
// Empresa:
//      Softech Sistemas
// Autor:
//      Softech Sistemas
// Fecha:
//      17 de Septiembre de 2.008
// Actualización: 09/08/2018
//      Cambio en el diccionario del Generador para el manejo correcto
//      de los tipos de objetos: String, DateTime, Decimal, Integer
// Actualización: 23/08/2018
//      Inclusión de la clase Utilitarios y adaptación en la invocación
//      de los métodos

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Text;
using utilitario = Softech.Nomina.Generacion.Utilitarios;

namespace Softech.Nomina.Generacion
{
    /// <summary>
    /// Clase para generar datos del sistema
    /// </summary>
    public static class BNCNO2
    {
        #region Generar

        /// <summary>
        /// Genera una cadena de caracteres con la informacion necesaria para generar el archivo de texto plano
        /// </summary>
        /// <param name="datos">DataSet con los datos necesarios para la generación de la cadena de caracteres</param>
        /// <param name="parametros">Parametros necesarios para la extracción de datos</param>
        /// <param name="ruta"></param>
        /// <param name="nombreArchivo"></param>
        public static void Generar(DataSet datos, Dictionary<String, Object> parametros, String ruta, String nombreArchivo)
        {
            DataSet ds = default(DataSet);

            #region Validar Objeto

            if (!(datos is DataSet))
                throw new Exception("Es necesario que el objeto de datos sea de tipo DataSet.");
            else
                ds = (DataSet)datos;

            if (ds.Tables.Count.Equals(0))
                throw new Exception("No existen tablas en el DataSet.");

            if (!ds.Tables.Contains("Nomina"))
                throw new Exception("No existen la tabla Fideicomiso en el DataSet.");

            if (ds.Tables["Nomina"].Rows.Count.Equals(0))
                throw new Exception("No existen datos que imprimir.");

            #endregion

            #region Preparar el texto

            StringBuilder texto = new StringBuilder();

            for (int i = 0; i < ds.Tables["Nomina"].Rows.Count; i++)
            {
                DataRow r = ds.Tables["Nomina"].Rows[i];

                if (i > 0)
                    texto.Append(Environment.NewLine);
                else
                {
                    #region Preparar el encabezado

                    // Registro de Control
                    texto.Append(utilitario.PrepararCadena((String)parametros["stiporeg"], 1,caracterRelleno.espacio, direccionRelleno.derecha,true));
                    texto.Append(utilitario.PrepararEntero(ds.Tables["Nomina"].Rows.Count, 5, caracterRelleno.cero, direccionRelleno.izquierda));
                    Decimal total = 0;
                    foreach (DataRow dr in ds.Tables["Nomina"].Rows)
                        total += Convert.ToDecimal(dr["monto"]);
                    texto.Append(utilitario.PrepararDecimal(total, 13, 2,caracterRelleno.cero));
                    texto.Append(utilitario.PrepararCadena((String)parametros["sasoccomercial"], 10, caracterRelleno.espacio, direccionRelleno.derecha, true));
                    texto.Append(utilitario.PrepararCadena((String)parametros["sestandaredifact"], 3, caracterRelleno.espacio, direccionRelleno.derecha, true));
                    texto.Append(utilitario.PrepararCadena((String)parametros["sversestandaredifact"], 2, caracterRelleno.espacio, direccionRelleno.derecha, true));
                    //texto.Append(utilitario.PrepararCadena((String)parametros["stipodocumento"], 6, caracterRelleno.espacio, direccionRelleno.derecha, true));
                    //texto.Append(utilitario.PrepararCadena((String)parametros["sproduccion"], 1, caracterRelleno.espacio, direccionRelleno.derecha, true));
                    texto.Append(Environment.NewLine);
                    texto.Append(Environment.NewLine);

                    /*
                    // Registro de Encabezado
                    texto.Append(utilitario.PrepararEntero((Int32)parametros["itiporegencab"], 2, caracterRelleno.cero, direccionRelleno.izquierda));
                    texto.Append(utilitario.PrepararCadena((String)parametros["stipotransaccion"], 35, caracterRelleno.espacio, direccionRelleno.derecha, true));
                    texto.Append(utilitario.PrepararCadena((String)parametros["scondordenpago"], 3, caracterRelleno.espacio, direccionRelleno.derecha, true));
                    texto.Append(utilitario.PrepararCadena((String)parametros["snroordenpago"], 35, caracterRelleno.espacio, direccionRelleno.derecha, true));
                    texto.Append(utilitario.PrepararFechaHora(DateTime.Now,false, formatoSalida.formaYYYYMMDDHHMMSS));

                    texto.Append(Environment.NewLine);

                    // Registro de debito
                    texto.Append(utilitario.PrepararEntero((Int32)parametros["itiporegdebito"], 2, caracterRelleno.cero, direccionRelleno.izquierda));
                    texto.Append(utilitario.PrepararCadena(utilitario.PrepararCadena((String)parametros["snrorefdebito"], 8,caracterRelleno.cero,direccionRelleno.izquierda), 30,caracterRelleno.espacio,direccionRelleno.derecha,true));
                    //texto.Append(PrepararCadena(PrepararEntero(parametros["snrorefdebito"], 8), 30));
                    texto.Append(r["rif_emp"].ToString().Substring(0, 1));
                    texto.Append(utilitario.PrepararCedulaRif(r["rif_emp"].ToString(), 16,caracterRelleno.espacio,direccionRelleno.derecha));
                    texto.Append(utilitario.PrepararCadena(r["nom_emp"].ToString().Replace(',', ' ').Replace('.', ' '), 35,caracterRelleno.espacio,direccionRelleno.derecha,true));

                    //Decimal total = 0;
                    //foreach (DataRow dr in ds.Tables["Nomina"].Rows)
                        //total += Convert.ToDecimal(dr["monto"]);

                    //exto.Append(utilitario.PrepararDecimal(total, 13, 2,caracterRelleno.cero));
                    texto.Append(utilitario.PrepararCadena((String)parametros["smoneda"], 3, caracterRelleno.espacio, direccionRelleno.derecha, true));
                    texto.Append(utilitario.PrepararCadena((String)parametros["scampolibre"], 1, caracterRelleno.espacio, direccionRelleno.derecha, true));
                    texto.Append(utilitario.PrepararCadena((String)parametros["snroctadebitar"], 34, caracterRelleno.espacio, direccionRelleno.derecha, true));
                    texto.Append(utilitario.PrepararCadena((String)parametros["scodbancoordenante"], 11, caracterRelleno.espacio, direccionRelleno.derecha, true));
                    texto.Append(utilitario.PrepararEntero((Int32)parametros["dfechaefectivapago"], 8, caracterRelleno.cero, direccionRelleno.izquierda));
                    //texto.Append(utilitario.PrepararFecha((DateTime)parametros["dfechaefectivapago"],tipoSalida.formaYYYYMMDD));
                    texto.Append(Environment.NewLine);
                    */

                    #endregion
                }

                // Registro de Crédito
                //texto.Append(utilitario.PrepararEntero((Int32)parametros["itiporegcredito"], 2, caracterRelleno.cero, direccionRelleno.izquierda));
                texto.Append(utilitario.PrepararCadena((String)parametros["sproduccion"], 1, caracterRelleno.espacio, direccionRelleno.derecha, true));
                texto.Append(utilitario.PrepararEntero((Int32)parametros["dfechaefectivapago"], 8, caracterRelleno.cero, direccionRelleno.izquierda));
                //texto.Append(utilitario.PrepararCadena((String)parametros["snroctadebitar"], 20, caracterRelleno.espacio, direccionRelleno.derecha, true));
                texto.Append(utilitario.PrepararCadena(r["cuentabanco"].ToString(), 20,caracterRelleno.espacio,direccionRelleno.derecha,true));
                texto.Append(utilitario.PrepararCadena(r["num_cta"].ToString(), 20,caracterRelleno.espacio,direccionRelleno.derecha,true));
                texto.Append(utilitario.PrepararDecimal((Decimal)r["monto"], 13, 2,caracterRelleno.cero));
                texto.Append(utilitario.PrepararCadena((String)parametros["stipotransaccion"], 60, caracterRelleno.espacio, direccionRelleno.derecha, true));
                //oj el parametro reci_num no existe , existe numdoc
                //ojo se comenta porque este campo no existe en la config  del extractor y se reemplaza por la linead e abajo: texto.Append(utilitario.PrepararCadena(utilitario.PrepararCadena((String)parametros["reci_num"], 8, caracterRelleno.cero, direccionRelleno.izquierda), 30, caracterRelleno.espacio, direccionRelleno.derecha, true));
                ///texto.Append(utilitario.PrepararCadena(utilitario.PrepararCadena("", 8, caracterRelleno.cero, direccionRelleno.izquierda), 30, caracterRelleno.espacio, direccionRelleno.derecha, true));
                //texto.Append(PrepararCadena(PrepararEntero(r["reci_num"].ToString(), 8), 30));
                //texto.Append(utilitario.PrepararCadena(Convert.ToString(utilitario.PrepararEntero(Convert.ToInt32 (r["reci_num"].ToString()), 8,caracterRelleno.cero,direccionRelleno.izquierda)), 30,caracterRelleno.espacio,direccionRelleno.derecha,true));
                //texto.Append(utilitario.PrepararDecimal((Decimal)r["monto"], 13, 2,caracterRelleno.cero));
                //texto.Append(utilitario.PrepararCadena((String)parametros["smoneda"], 3, caracterRelleno.espacio, direccionRelleno.derecha, true));
                //texto.Append(utilitario.PrepararCadena(r["num_cta"].ToString(), 30,caracterRelleno.espacio,direccionRelleno.derecha,true));
                //texto.Append(utilitario.PrepararCadena(r["num_cta"].ToString(), 4,caracterRelleno.espacio,direccionRelleno.derecha,true));
                //texto.Append(utilitario.PrepararCadena((String)parametros["sbancobenef"], 7, caracterRelleno.espacio, direccionRelleno.derecha, true));
                //texto.Append(utilitario.PrepararCadena((String)parametros["scodigoagencia"], 3, caracterRelleno.espacio, direccionRelleno.derecha, true));
                texto.Append(r["nac"].ToString().Substring(0, 1) == "1" ? "V" + "0" : "E" + "0");
                texto.Append(utilitario.PrepararCedulaRif(r["ci"].ToString().TrimEnd(),8,caracterRelleno.cero,direccionRelleno.izquierda));
                texto.Append(utilitario.PrepararCadena(r["nombres"].ToString() + " " + r["apellidos"].ToString(), 35,caracterRelleno.espacio,direccionRelleno.derecha,true));
                //texto.Append(" ");
                //texto.Append(utilitario.PrepararCadena("", 7,caracterRelleno.espacio,direccionRelleno.derecha,false));
                /*
                texto.Append(utilitario.PrepararCadena(r["nombres"].ToString() + " " + r["apellidos"].ToString(), 70,caracterRelleno.espacio,direccionRelleno.derecha,true));
                */

                texto.Append(utilitario.PrepararCadena(String.Empty, 45, caracterRelleno.espacio, direccionRelleno.derecha, true));
                //texto.Append(utilitario.PrepararCadena(r["correo_e"].ToString(), 40, caracterRelleno.espacio, direccionRelleno.derecha));
				texto.Append(utilitario.PrepararCadena("dasilva.master2008@gmail.com", 40, caracterRelleno.espacio, direccionRelleno.derecha));
                //texto.Append(PrepararCadena(r["direccion"].ToString(), 70));
                texto.Append(utilitario.PrepararCadena(String.Empty, 70, caracterRelleno.espacio, direccionRelleno.derecha, true));
                //texto.Append(utilitario.PrepararCadena(String.Empty, 25, caracterRelleno.espacio, direccionRelleno.derecha, false));
                //texto.Append(utilitario.PrepararCadena(String.Empty, 17, caracterRelleno.espacio, direccionRelleno.derecha, false));
                /*
                texto.Append(utilitario.PrepararCadena(r["avisar_a"].ToString(), 35, caracterRelleno.espacio, direccionRelleno.derecha, true));
                */
                //texto.Append(utilitario.PrepararCadena(String.Empty, 1, caracterRelleno.espacio, direccionRelleno.derecha, false));
                //texto.Append(PrepararEntero(parametros["icalificadorfideic"], 1));
                //texto.Append(utilitario.PrepararCadena((String)parametros["sfichaempleado"], 30, caracterRelleno.espacio, direccionRelleno.derecha, true));
                //texto.Append(utilitario.PrepararCadena((String)parametros["stiponomina"], 2, caracterRelleno.espacio, direccionRelleno.derecha, true));
                //texto.Append(utilitario.PrepararCadena((String)parametros["subicaciongeografica"], 21, caracterRelleno.espacio, direccionRelleno.derecha, true));
                //texto.Append(utilitario.PrepararCadena((String)parametros["sformapago"], 3, caracterRelleno.espacio, direccionRelleno.derecha, true));

            }

            #region Preparar Pie de Pagina

            //texto.Append(Environment.NewLine);
            //texto.Append(utilitario.PrepararEntero((Int32)parametros["itiporegtotales"], 2, caracterRelleno.cero, direccionRelleno.izquierda));
            //texto.Append(utilitario.PrepararEntero(1, 15, caracterRelleno.cero, direccionRelleno.izquierda));
            //texto.Append(utilitario.PrepararEntero(ds.Tables["Nomina"].Rows.Count, 15, caracterRelleno.cero, direccionRelleno.izquierda));

            Decimal montoTotal = 0;

            foreach (DataRow dr in ds.Tables["Nomina"].Rows)
                montoTotal += Convert.ToDecimal(dr["monto"]);

            //texto.Append(utilitario.PrepararDecimal(montoTotal, 13, 2,caracterRelleno.cero));

            Decimal montoTotal2 = 0;
            foreach (DataRow dr in ds.Tables["Nomina"].Rows)
                montoTotal2 += Convert.ToDecimal(dr["monto"]);

            //texto.Append(PrepararDecimal(montoTotal2.ToString(), 13, 2));
            //texto.Append(PrepararDecimal(montoTotal2.ToString(), 13, 2) + "\n");
            texto.Append(Environment.NewLine);


            #endregion

            #endregion

            #region Crear y escribir el archivo

            if (!ruta[ruta.Length - 1].Equals("\\"))
                ruta += "\\";

            using (StreamWriter sw = new StreamWriter(ruta + nombreArchivo, false, Encoding.Default))
            {
                sw.Write(texto.ToString());
                sw.Flush();
            }

            #endregion
        }

        #endregion

    }
}