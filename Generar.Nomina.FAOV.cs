// INFORMACIÓN
//
// Nombre:
//      Generador de Datos para la ley de política habitacional 
//      del banco Fondo Común
// Descripción:
//      Archivo de código para la generacion de los datos para
//      la ley de política habitacional del banco Fondo Común.
// Empresa:
//      Softech Sistemas
// Autor:
//      Softech Sistemas
// Fecha:
//      15 de Septiembre de 2.008
// Actualización: 08/08/2018
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
    public static class FAOV
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
            DataSet ds = datos;
            String fecha = String.Empty;
            #region Validar Objeto

            if (ds.Tables.Count.Equals(0))
                throw new Exception("No existen tablas en el DataSet.");

            if (!ds.Tables.Contains("faov"))
                throw new Exception("No existen la tabla Fideicomiso en el DataSet.");

            if (ds.Tables["faov"].Rows.Count.Equals(0))
                throw new Exception("No existen datos que imprimir.");

            #endregion

            #region Preparar el texto

            StringBuilder texto = new StringBuilder();

            for (int i = 0; i < ds.Tables["faov"].Rows.Count; i++)
            {
                if (i > 0)
                    texto.Append(Environment.NewLine);

                DataRow r = ds.Tables["faov"].Rows[i];

                texto.Append(r["nac"].ToString() + ",");
                texto.Append(utilitario.PrepararNombreCompuesto(utilitario.QuitarCaracteresEspeciales(r["ci"].ToString().Trim())) + ","); //texto.Append(PrepararCadena2(r["ci"].ToString().Trim()) + ",");

                #region codigo que no se estaba usando
                
                #endregion
                
                //CER SE MODIFICO PARA LAS PERSONAS CON NOMBRES COMPUESTOS EJEMPLO MARIA DE LOS ANGELES
                var Nom = r["nombres"].ToString().Split(' ');

                texto.Append(utilitario.PrepararNombreCompuesto(Nom[0]) + ","); //texto.Append(QuitarCaracteresEspeciales2(Nom[0]) + ",");
                for (int n = 1; Nom.Length > n; n++)
                    texto.Append(utilitario.PrepararNombreCompuesto(Nom[n]) + " ");//texto.Append(QuitarCaracteresEspeciales2(Nom[n]) + " ");
                texto.Replace(' ', ',', texto.Length - 1, 1);
                if (Nom.Length == 1)
                    texto.Append(",");
                //Para los apellidos
                var Ape = r["apellidos"].ToString().Split(' ');
                texto.Append(utilitario.PrepararNombreCompuesto(Ape[0]) + ","); //texto.Append(QuitarCaracteresEspeciales2(Ape[0]) + ",");
                for (int a = 1; Ape.Length > a; a++)
                    texto.Append(utilitario.PrepararNombreCompuesto(Ape[a]) + " "); //texto.Append(QuitarCaracteresEspeciales2(Ape[a]) + " ");
                texto.Replace(' ', ',', texto.Length - 1, 1);
                if (Ape.Length == 1)
                    texto.Append(",");
                //FIN CER

                texto.Append(utilitario.PrepararNombreCompuesto(utilitario.QuitarCaracteresEspeciales(r["SALARIO_INT"].ToString().Trim())) + ","); //texto.Append(PrepararCadena2(r["SALARIO_INT"].ToString().Trim()) + ",");
                texto.Append(utilitario.PrepararFecha((DateTime)r["fecha_ing"], tipoSalida.formaDDMMYYYY) + ",");
                if (r["fecha_egr"].ToString() != "")
                    texto.Append(utilitario.PrepararFecha((DateTime)r["fecha_egr"], tipoSalida.formaDDMMYYYY));

                #region codigo que no se estaba usando2
            
                #endregion
            }

            #endregion

            #region Crear y escribir el archivo

            DataRow rs = ds.Tables["faov"].Rows[0];
            var cuenta = rs["cuentabanco"].ToString();
            
            if (!ruta[ruta.Length - 1].Equals("\\"))
                ruta += "\\";
            var mes = utilitario.PrepararEntero(Convert.ToDateTime(parametros["fechas"]).Month,2, caracterRelleno.cero,direccionRelleno.izquierda); //var mes = parametros["fechas"].Substring(3, 2);
            var anho = utilitario.PrepararEntero(Convert.ToDateTime(parametros["fechas"]).Year,4,caracterRelleno.cero,direccionRelleno.izquierda); //var anho = parametros["fechas"].Substring(6, 4);
       
            String cta = utilitario.PrepararCuentaBancaria(cuenta, 20,caracterRelleno.cero,direccionRelleno.izquierda);
                   
            nombreArchivo = "N" + cta + mes + anho + ".txt";
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