# Kivly: conexion minima de backend

Para alcance, decisiones de producto, contratos de clientes y bloqueos del alta
dual, consultar [KIVLY_INTEGRATION.md](KIVLY_INTEGRATION.md). Esta pagina describe
solo el cliente de conexion; no acredita registro en el club ni entrega de tarjeta.
La prueba separada `scripts\probe_kivly_registration.py` si puede crear una
ficha con confirmacion explicita en un negocio de pruebas; ver sus precauciones
en la guia de integracion. El comprobador de conexion de esta pagina no cambia.

Esta pagina describe el comprobador original de token/contexto, que no crea
clientes ni accede a AWS. El cliente reutilizable ahora tambien soporta operaciones
de clientes usadas por el alta dual y catalogo/asignacion de premios al terminar
STACK. Estas operaciones comprueban alcance de ubicacion y propietario; el
telefono solo se incluye al crear cliente si se ha proporcionado.
Sus Lambdas, secretos, tabla y condiciones de
activacion estan descritos en la guia de integracion. No confundir esos flujos
con la comprobacion local de esta pagina.

## Comprobacion local

Desde la raiz del backend, usando una terminal interactiva y Python 3.13:

```powershell
python .\scripts\check_kivly_connection.py
```

Solicita `client_id` y `client_secret` con entrada oculta. Usar las credenciales
de ubicacion del panel normal de Kivly, no la contrasena personal ni OAuth de
otro proveedor. No pegarlas en chat, argumentos, `.env`, logs o repositorio.
La herramienta no acepta credenciales por argumentos ni por tuberia.

Solo realiza `POST https://api.getkivly.com/api/v1/auth/token` y
`GET https://api.getkivly.com/api/v1/integration/context`. No consulta contactos,
catalogo ni sucursales y no sigue redirecciones ni reintenta automaticamente.
Solicitar un token no es una escritura de negocio.

Ante una CA corporativa, usar un PEM de confianza sin desactivar TLS:

```powershell
python .\scripts\check_kivly_connection.py --ca-bundle "C:\ruta\ca-confiable.pem"
```

El resumen solo muestra limites, numero de recursos y coherencia de alcance entre
token y contexto. No imprime token, credenciales, nombres, IDs o cuerpos externos.
No demuestra que sea el negocio que querias conectar, permisos de escritura,
membresia de club ni efectos comerciales.

El 24 de octubre de 2026 el usuario ejecuto la comprobacion local en produccion:
autenticacion y contexto correctos, alcance coherente entre ambos, caducidad
maxima de 3600 segundos, limites de 120 peticiones/minuto y 5000/hora, y 31
recursos declarados. No se realizaron escrituras de negocio. Esta comprobacion
no valida el negocio esperado ni permisos o efectos de futuras operaciones.

Codigos de salida: `0` comprobacion correcta; `1` credenciales, contrato,
permisos, TLS o red requieren revision; `2` entrada interactiva segura no disponible
o argumentos invalidos.

## Uso en servidor

Importar `KivlyClient` de `stand_common.kivly`, construirlo con credenciales
obtenidas por el mecanismo seguro del servidor y llamar `get_context()`.
Devuelve `IntegrationContext` con los campos publicados de contexto. El llamador
debe comprobar el negocio esperado antes de autorizar cualquier operacion futura
y no publicar el objeto completo ni registrarlo indiscriminadamente.

El token se obtiene de `data.token`; se valida Bearer, alcance y caducidad.
Cada instancia conserva solo en memoria su token y credenciales. Reutiliza el
token hasta 30 segundos antes de la fecha mas temprana entre `expires_in` y
`expires_at`; los valores por defecto del esquema son Bearer y 3600 segundos
cuando los campos opcionales faltan. No hay cache global, refresh token, cambio
de credenciales en una instancia ni garantia de sincronizacion entre hilos.
Al rotar credenciales, crear otra instancia.

La configuracion de destino queda fija en produccion; staging requiere otro
contrato/provisionamiento y no se habilita con una URL arbitraria.
TLS se verifica, timeout por peticion de 8 segundos, respuesta maxima de 1 MiB.
Ambas peticiones se identifican con `User-Agent: Stand-Kivly/1.0`, sin suplantar
un navegador. En la comparacion local, cURL obtuvo 200 con su identificacion
habitual y 403 al usar `Python-urllib/3.13`. Esto identifica el User-Agent como
factor del rechazo observado, no como requisito universal publicado por Kivly.
El usuario confirmo despues autenticacion y contexto correctos con
`Stand-Kivly/1.0` usando el cliente Python local.

Los errores `KivlyError` exponen solo `code`, `http_status` y `retry_after`.
401 invalida el token para una llamada posterior; 403 no provoca renovaciones.
429/503 pueden aportar `retry_after` en segundos. El cliente no duerme ni reintenta:
el llamador debe decidir una politica acotada cuando exista un caso real.
No capturar errores para devolver contexto vacio o exito. El llamador es responsable
de notificar o registrar el codigo sanitizado, sin cuerpos ni secretos.

## Validacion y fuentes

Las pruebas simuladas de cuenta/configuracion no acreditan un alta real.
La lista condensada de escenarios a comprobar y el procedimiento del frontend
local estan en [KIVLY_INTEGRATION.md](KIVLY_INTEGRATION.md#verificacion-del-alta-dual).
Para comprobaciones reales de conexion, usar el script interactivo descrito
arriba, sin imprimir credenciales ni respuestas completas.

Contrato consultado el 24 de octubre de 2026, Kivly Public API 1.1.0:

- [Autenticacion](https://kivly.readme.io/reference/login_with_client_id_and_secret_api_v1_auth_token_post.md)
- [Contexto](https://kivly.readme.io/reference/integration_context_api_v1_integration_context_get.md)
- [Indice](https://kivly.readme.io/llms.txt)

La referencia publica documenta 3600 segundos sin refresh token y limites por
ubicacion compartidos entre tokens/servidores. El contexto devuelve los presupuestos
efectivos. Obtener un token no concede administracion del panel ni GraphQL.
Si el despliegue real difiere del esquema, revisar con Kivly; no aceptar
silenciosamente el antiguo `access_token` de primer nivel.
