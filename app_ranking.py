# ---------------------------------------------------------------
# TAB 2: RANKING
# ---------------------------------------------------------------
with tab_ranking:
    df_rutas = leer_rutas()
    df_contexto = leer_contexto()

    if df_rutas.empty:
        st.info("Carga primero el archivo Diario en la pestaña anterior.")
    else:
        fecha_max = df_rutas["Fecha"].max()
        fecha_min_datos = df_rutas["Fecha"].min()
        un_dia = timedelta(days=1)

        # ---------- PERIODO ----------
        st.markdown("#### 🗓️ Periodo a validar")
        opcion_periodo = st.selectbox(
            "Periodo",
            ["Hoy", "Ayer", "Últimos 7 días", "Semana anterior (7 días previos)",
             "Últimos 30 días", "Mes anterior (30 días previos)", "Rango personalizado"],
            key="rk_periodo",
            help=f"'Hoy' toma como referencia la fecha más reciente cargada ({fecha_max}), no la del calendario.",
        )

        if opcion_periodo == "Hoy":
            fecha_ini = fecha_fin = fecha_max
        elif opcion_periodo == "Ayer":
            fecha_ini = fecha_fin = fecha_max - un_dia
        elif opcion_periodo == "Últimos 7 días":
            fecha_ini, fecha_fin = fecha_max - timedelta(days=6), fecha_max
        elif opcion_periodo.startswith("Semana anterior"):
            fecha_ini, fecha_fin = fecha_max - timedelta(days=13), fecha_max - timedelta(days=7)
        elif opcion_periodo == "Últimos 30 días":
            fecha_ini, fecha_fin = fecha_max - timedelta(days=29), fecha_max
        elif opcion_periodo.startswith("Mes anterior"):
            fecha_ini, fecha_fin = fecha_max - timedelta(days=59), fecha_max - timedelta(days=30)
        else:
            cP1, cP2 = st.columns(2)
            with cP1:
                fecha_ini = st.date_input("Desde", value=fecha_max - timedelta(days=6), key="rk_desde")
            with cP2:
                fecha_fin = st.date_input("Hasta", value=fecha_max, key="rk_hasta")

        st.caption(f"Evaluando del **{fecha_ini}** al **{fecha_fin}** · tus datos cargados van del {fecha_min_datos} al {fecha_max}.")

        if fecha_ini > fecha_fin:
            st.error("La fecha 'Desde' es posterior a 'Hasta'.")
        elif fecha_fin < fecha_min_datos:
            st.warning(
                f"No hay datos cargados para ese periodo: tu histórico empieza el {fecha_min_datos}. "
                "Sube archivos Diario de fechas anteriores para poder consultarlo."
            )
        else:
            if fecha_ini < fecha_min_datos:
                st.info(f"Este periodo está cubierto solo parcialmente: hay datos desde el {fecha_min_datos}.")

            # ---------- METRICA Y FILTROS ----------
            m1, m2, m3, m4 = st.columns(4)
            with m1:
                nombre_metrica = st.selectbox(
                    "Métrica del ranking", list(METRICAS.keys()), key="rk_metrica",
                    help="% Gestión se calcula del Diario en el periodo elegido. Las demás vienen del archivo General.",
                )
            with m2:
                min_rutas = st.number_input("Rutas mínimas realizadas", min_value=1, max_value=200, value=1, key="rk_min_rutas")
            with m3:
                umbral_paq = st.slider("Paquetes mínimos", 0, 100, UMBRAL_RUTAS_DEFAULT, key="rk_umbral")
            with m4:
                n_top = st.selectbox("Tamaño Top/Bottom", [5, 10, 15, 20], index=1, key="rk_ntop")
            metrica = METRICAS[nombre_metrica]

            df_rank = calcular_ranking(df_rutas, df_contexto, fecha_ini, fecha_fin, umbral_paq, [], [], min_rutas=min_rutas)

            if df_rank.empty:
                st.warning("No hay aliados con rutas realizadas en este periodo con esos mínimos.")
            else:
                df_m = df_rank.dropna(subset=[metrica]).copy()
                sin_dato = len(df_rank) - len(df_m)
                if sin_dato:
                    st.caption(f"ℹ️ {sin_dato} aliados cargaron rutas pero no aparecen en el archivo General, así que no entran a este ranking.")

                if df_m.empty:
                    st.warning("Ningún aliado del periodo tiene esa métrica. Carga el archivo General o elige '% Gestión'.")
                else:
                    cols_mostrar = ["Aliado", "Ciudad", "Categoria", "Rutas_en_rango", "Tot_Paq",
                                    "Pct_Gestion", "Efectividad_General", "Cumplimiento", "Uso_Boton_VPA", "Rendim_Global"]

                    k1, k2, k3, k4 = st.columns(4)
                    k1.metric("Aliados con rutas realizadas", len(df_rank))
                    k2.metric(f"Promedio {nombre_metrica.split(' (')[0]}", f"{df_m[metrica].mean()*100:.1f}%")
                    k3.metric("Rutas realizadas en el periodo", int(df_rank["Rutas_en_rango"].sum()))
                    k4.metric(
                        "Aliados activos (60 días)",
                        int((df_rank["Ultima_fecha"] >= (fecha_max - timedelta(days=60))).sum()),
                        help="Activo = tuvo al menos una ruta en los últimos 60 días respecto al dato más reciente cargado.",
                    )

                    st.download_button(
                        "⬇️ Descargar Excel: TODOS los aliados y su %",
                        data=generar_excel_ranking(df_m, metrica, nombre_metrica, n_top, cols_mostrar, fecha_ini, fecha_fin),
                        file_name=f"ranking_aliados_{fecha_ini}_{fecha_fin}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        type="primary",
                        key="rk_descarga",
                    )
                    st.caption("El Excel trae: lista completa con puesto nacional / en su ciudad / en su categoría, Top y Bottom nacional, y Top y Bottom por ciudad y por categoría.")

                    # ---------- NACIONAL ----------
                    st.divider()
                    st.markdown(f"### 🏆 Top / Bottom Nacional — {nombre_metrica}")
                    top_nac, bottom_nac = top_bottom(df_m, n=n_top, metrica=metrica)
                    colA, colB = st.columns(2)
                    with colA:
                        st.markdown(f"**🟢 Top {n_top} — mejor %**")
                        st.dataframe(formatear_tabla(top_nac, cols_mostrar), use_container_width=True, hide_index=True)
                    with colB:
                        st.markdown(f"**🔴 Bottom {n_top} — peor %**")
                        st.dataframe(formatear_tabla(bottom_nac, cols_mostrar), use_container_width=True, hide_index=True)

                    # ---------- POR CIUDAD ----------
                    st.divider()
                    st.markdown("### 🏙️ Ranking por ciudad")
                    ciudad_focus = st.selectbox("Ciudad", sorted(df_m["Ciudad"].dropna().unique()), key="rk_ciudad")
                    top_c, bottom_c = top_bottom(df_m, n=n_top, por_ciudad=ciudad_focus, metrica=metrica)
                    colC, colD = st.columns(2)
                    with colC:
                        st.markdown(f"**🟢 Top {n_top} en {ciudad_focus}**")
                        st.dataframe(formatear_tabla(top_c, cols_mostrar), use_container_width=True, hide_index=True)
                    with colD:
                        st.markdown(f"**🔴 Bottom {n_top} en {ciudad_focus}**")
                        st.dataframe(formatear_tabla(bottom_c, cols_mostrar), use_container_width=True, hide_index=True)

                    with st.expander(f"📋 Lista completa de {ciudad_focus} (todos los aliados, ordenados)"):
                        lista_ciudad = df_m[df_m["Ciudad"] == ciudad_focus].sort_values(metrica, ascending=False)
                        lista_ciudad_fmt = formatear_tabla(lista_ciudad, cols_mostrar)
                        lista_ciudad_fmt.insert(0, "Puesto", range(1, len(lista_ciudad_fmt) + 1))
                        st.dataframe(lista_ciudad_fmt, use_container_width=True, hide_index=True)

                    # ---------- POR CATEGORIA ----------
                    st.divider()
                    st.markdown("### 🏷️ Ranking por categoría")
                    cats_disp = sorted(
                        df_m["Categoria"].dropna().unique(),
                        key=lambda c: CATEGORIAS_ORDEN.index(c) if c in CATEGORIAS_ORDEN else 99,
                    )
                    categoria_focus = st.selectbox("Categoría", cats_disp, key="rk_categoria")
                    df_cat = df_m[df_m["Categoria"] == categoria_focus].sort_values(metrica, ascending=False)
                    top_cat = df_cat.head(n_top)
                    bottom_cat = df_cat.tail(n_top).sort_values(metrica, ascending=True)
                    colE, colF = st.columns(2)
                    with colE:
                        st.markdown(f"**🟢 Top {n_top} en {categoria_focus}**")
                        st.dataframe(formatear_tabla(top_cat, cols_mostrar), use_container_width=True, hide_index=True)
                    with colF:
                        st.markdown(f"**🔴 Bottom {n_top} en {categoria_focus}**")
                        st.dataframe(formatear_tabla(bottom_cat, cols_mostrar), use_container_width=True, hide_index=True)

                    with st.expander(f"📋 Lista completa de la categoría {categoria_focus} (todos los aliados, ordenados)"):
                        lista_cat_fmt = formatear_tabla(df_cat, cols_mostrar)
                        lista_cat_fmt.insert(0, "Puesto", range(1, len(lista_cat_fmt) + 1))
                        st.dataframe(lista_cat_fmt, use_container_width=True, hide_index=True)

                    # ---------- LISTA COMPLETA ----------
                    st.divider()
                    with st.expander("📋 Ver TODOS los aliados del periodo, agrupados por ciudad"):
                        todos = agregar_puestos(df_m, metrica)
                        st.dataframe(
                            formatear_tabla(todos, ["Puesto_Ciudad", "Puesto_Nacional"] + cols_mostrar),
                            use_container_width=True, hide_index=True,
                        )


# ---------------------------------------------------------------
# TAB 3: COMPARACION ENTRE PERIODOS
# ---------------------------------------------------------------
with tab_comparar:
    df_rutas = leer_rutas()
    df_contexto = leer_contexto()

    if df_rutas.empty:
        st.info("Carga primero el archivo Diario para poder comparar periodos.")
    else:
        fecha_max = df_rutas["Fecha"].max()
        fecha_min_datos = df_rutas["Fecha"].min()

        st.markdown(
            "Compara el **% Gestión** de los aliados entre dos periodos para llevar el control: "
            "quién mejoró, quién empeoró, quién es nuevo y quién dejó de cargar."
        )
        modo = st.selectbox(
            "Comparación",
            ["Hoy vs. ayer", "Últimos 7 días vs. 7 días anteriores",
             "Últimos 30 días vs. 30 días anteriores", "Personalizado"],
            key="cp_modo",
        )

        if modo == "Hoy vs. ayer":
            b_ini = b_fin = fecha_max
            a_ini = a_fin = fecha_max - timedelta(days=1)
        elif modo.startswith("Últimos 7"):
            b_ini, b_fin = fecha_max - timedelta(days=6), fecha_max
            a_ini, a_fin = fecha_max - timedelta(days=13), fecha_max - timedelta(days=7)
        elif modo.startswith("Últimos 30"):
            b_ini, b_fin = fecha_max - timedelta(days=29), fecha_max
            a_ini, a_fin = fecha_max - timedelta(days=59), fecha_max - timedelta(days=30)
        else:
            cA, cB = st.columns(2)
            with cA:
                st.markdown("**Periodo A (anterior)**")
                a_ini = st.date_input("Desde", key="cp_a_ini", value=fecha_max - timedelta(days=13))
                a_fin = st.date_input("Hasta", key="cp_a_fin", value=fecha_max - timedelta(days=7))
            with cB:
                st.markdown("**Periodo B (reciente)**")
                b_ini = st.date_input("Desde", key="cp_b_ini", value=fecha_max - timedelta(days=6))
                b_fin = st.date_input("Hasta", key="cp_b_fin", value=fecha_max)

        st.caption(
            f"Periodo A (anterior): **{a_ini} → {a_fin}** · Periodo B (reciente): **{b_ini} → {b_fin}** · "
            f"datos cargados: {fecha_min_datos} → {fecha_max}"
        )

        if a_ini > a_fin or b_ini > b_fin:
            st.error("En alguno de los periodos la fecha 'Desde' es posterior a 'Hasta'.")
        elif a_ini <= b_fin and b_ini <= a_fin:
            st.warning(
                "Los dos periodos se traslapan (comparten fechas), por eso las diferencias salen en 0. "
                "Ajusta las fechas para que el periodo A termine antes de que empiece el B."
            )
        elif a_fin < fecha_min_datos:
            st.warning(
                f"No hay datos cargados para el periodo A: tu histórico empieza el {fecha_min_datos}. "
                "Sube archivos Diario de fechas anteriores o elige una comparación más corta."
            )
        else:
            if a_ini < fecha_min_datos:
                st.info(f"El periodo A está cubierto solo parcialmente (hay datos desde el {fecha_min_datos}).")

            f1, f2, f3 = st.columns(3)
            with f1:
                umbral_comp = st.slider("Paquetes mínimos por periodo", 0, 100, UMBRAL_RUTAS_DEFAULT, key="cp_umbral")
            with f2:
                ciudades_cp = st.multiselect(
                    "Ciudad (vacío = todas)", sorted(df_rutas["Ciudad"].dropna().unique()), key="cp_ciudades"
                )
            with f3:
                n_cp = st.selectbox("Tamaño de las listas", [5, 10, 15, 20], index=1, key="cp_ntop")

            df_a = calcular_ranking(df_rutas, df_contexto, a_ini, a_fin, umbral_comp, [], ciudades_cp)
            df_b = calcular_ranking(df_rutas, df_contexto, b_ini, b_fin, umbral_comp, [], ciudades_cp)

            if df_a.empty or df_b.empty:
                st.warning("Alguno de los dos periodos no tiene aliados con esos mínimos de paquetes.")
            else:
                base_cols = ["Identificacion", "Aliado", "Ciudad", "Categoria", "Rutas_en_rango", "Pct_Gestion"]
                comp = df_a[base_cols].merge(df_b[base_cols], on="Identificacion", how="outer", suffixes=("_A", "_B"))
                for c in ["Aliado", "Ciudad", "Categoria"]:
                    comp[c] = comp[f"{c}_B"].fillna(comp[f"{c}_A"])
                comp["Pct_A"] = comp["Pct_Gestion_A"] * 100
                comp["Pct_B"] = comp["Pct_Gestion_B"] * 100
                comp["Delta_puntos"] = comp["Pct_B"] - comp["Pct_A"]

                nuevos = comp[comp["Pct_A"].isna()]
                salieron = comp[comp["Pct_B"].isna()]
                continuos = comp.dropna(subset=["Pct_A", "Pct_B"])

                k1, k2, k3, k4 = st.columns(4)
                k1.metric("Aliados nuevos en B", len(nuevos))
                k2.metric("Dejaron de cargar", len(salieron))
                k3.metric("Aliados continuos", len(continuos))
                k4.metric(
                    "% Gestión promedio (B vs A)",
                    f"{df_b['Pct_Gestion'].mean()*100:.1f}%",
                    delta=f"{(df_b['Pct_Gestion'].mean() - df_a['Pct_Gestion'].mean())*100:+.1f} pts",
                )

                cols_cp = ["Aliado", "Ciudad", "Categoria", "Pct_A", "Pct_B", "Delta_puntos"]
                colE, colF = st.columns(2)
                with colE:
                    st.markdown(f"### 📈 Los {n_cp} que más mejoraron")
                    st.dataframe(
                        continuos.sort_values("Delta_puntos", ascending=False).head(n_cp)[cols_cp].round(1),
                        use_container_width=True, hide_index=True,
                    )
                with colF:
                    st.markdown(f"### 📉 Los {n_cp} que más cayeron")
                    st.dataframe(
                        continuos.sort_values("Delta_puntos", ascending=True).head(n_cp)[cols_cp].round(1),
                        use_container_width=True, hide_index=True,
                    )

                with st.expander("📋 Comparación completa de todos los aliados"):
                    completa = continuos.sort_values("Delta_puntos", ascending=False)[cols_cp].round(1)
                    st.dataframe(completa, use_container_width=True, hide_index=True)

                st.download_button(
                    "⬇️ Descargar Excel de la comparación",
                    data=excel_bytes({
                        "Comparacion completa": completa if True else None,
                        "Nuevos en B": nuevos[["Aliado", "Ciudad", "Categoria", "Pct_B"]].round(1),
                        "Dejaron de cargar": salieron[["Aliado", "Ciudad", "Categoria", "Pct_A"]].round(1),
                    }),
                    file_name=f"comparacion_{a_ini}_{a_fin}_vs_{b_ini}_{b_fin}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="cp_descarga",
                )
