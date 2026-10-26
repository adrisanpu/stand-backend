# Kivly: integracion con Stand

Guia interna para retomar la integracion sin depender del historial del chat.
Referencia publica consultada el 24 de octubre de 2026: Kivly Public API 1.1.0.
No contiene credenciales ni datos de participantes.

## Estado real

El backend anterior esta desplegado en dev. Los cambios actuales de esta rama,
tanto de backend como de frontend, siguen locales y no se han desplegado. Preparar
PRs para revision antes de cualquier merge o despliegue. El codigo local incluye
conexion por organizador, consentimiento ligado al email verificado, alta y
reutilizacion de clientes para STACK, proteccion de acceso y premios/Wallet
posteriores al resultado STACK. No envia tarjetas por email. Un build o una prueba
simulada no acredita un alta, premio o Wallet real.

Existe ademas una prueba local de creacion controlada, descrita abajo. No forma
parte de las Lambdas y no habilita el registro en Stand. Se ha ejecutado una
creacion real autorizada en el negocio indicado como de pruebas; ver resultados.
El usuario confirmo en el panel que la ficha creada aparece como miembro del
club en ese negocio de pruebas; no extrapolarlo como garantia contractual general.

## Decisiones del producto

| Aspecto | Decision |
|---------|----------|
| Primer incremento | Documentacion y alta dual Stand/Kivly para STACK desde Play movil |
| Conexion | Una por organizador de Stand, compartida entre sus juegos |
| Acceso al juego | Solo STACK exige alta en el club, con aceptacion explicita de condiciones; otros juegos no consultan ni aplican Kivly |
| Clientes existentes | Reconocer por email verificado en Play, en el mismo negocio; reutilizar ficha sin sobrescribir datos |
| Fallo del alta | No permitir jugar hasta confirmar; recuperar sin crear duplicados |
| Formulario `/play` | Nombre completo primero, email obligatorio verificado y telefono opcional si se habilita; el nombre aparece en rankings |
| Telefono | Enviar solo si se proporciona y solo al crear cliente; no modificar fichas existentes |
| Wallet | Tras el resultado guardado de STACK, mostrar Apple/Google solo si Kivly devuelve sus enlaces HTTPS admitidos; no elegir plataforma durante el alta |
| Premios | Seleccionar un premio existente del negocio; el mismo para cada participante que termina STACK, incluso con cero puntos |
| Fallo de premio/Wallet | Conservar resultado; mostrar error, pendiente o tarjeta no disponible sin repetir escrituras inciertas |

Guardar la tarjeta es una accion voluntaria del jugador; no confundirla con
confirmar su alta. Asignar un premio tampoco equivale necesariamente a canjearlo
o gastar puntos. Se usa `earn`, nunca `redeem` ni `mark-used`.

## Premio y Wallet de STACK

- `GET /v1/game/kivly/rewards?gameId=...`: catalogo activo del negocio conectado,
  autorizado por propietario. Se pagina con `skip/limit`, usando `title` como nombre.
- `PUT /v1/game/kivly`: `{gameId,rewardId}` selecciona un premio de ese catalogo;
  `rewardId:null` lo retira. Solo STACK. Se guarda con el boton del juego.
- `POST /v1/public/kivly/reward`: `{gameId,playerToken}` exige token del jugador,
  membresia y resultado STACK persistido con intento finalizado. Devuelve
  `reward.status: none|pending|confirmed`, nombre y enlaces opcionales.
- El backend conserva una reserva durable por juego/jugador en la tabla de
  registro Kivly. Compara asignaciones anteriores y posteriores a
  `POST /api/v1/redemptions/rewards/earn`, confirmando solo una asignacion nueva
  del premio elegido tras respuesta correcta. Cambiar o retirar la seleccion
  no vuelve a premiar a un jugador ya procesado.
- Las tarjetas proceden de `appleWalletUrl` y `googleWalletUrl` en
  `GET /api/v1/redemptions/customer/{customer_id}` (array paginado por `page/limit`).
  No se construyen a partir de `apple_serial`. Se admiten HTTPS de dominios
  Kivly y los hosts Google Wallet admitidos por el backend. Un enlace ajeno o
  invalido provoca error explicito; no se publica como tarjeta.
- Timeout, lectura incompleta o multiples asignaciones nuevas dejan `pending`:
  comprobar desde la interfaz no repite `earn`. No hay recuperacion automatica
  de una escritura incierta; requiere conciliacion por operador. El contrato
  no ofrece correlacion/idempotencia: otra integracion que asigne el mismo premio
  simultaneamente puede impedir atribuir inequivocamente el resultado.
- Falta confirmar en el negocio de pruebas el efecto comercial de `earn`, sus
  estados y la generacion/disponibilidad efectiva de cada Wallet. No se promete
  que Apple y Google esten disponibles simultaneamente.

Comprobacion tras desplegar este incremento a dev: habilitar telefono, elegir un
premio existente, registrarse con email verificado y terminar STACK. Comprobar
cliente/nombre/telefono y premio en el panel Kivly, abrir las tarjetas disponibles
en dispositivos compatibles y recargar el resultado para comprobar que no duplica.
Repetir sin telefono y con cliente existente (sin modificarlo). Esta prueba hace
escrituras reales: usar solo el negocio y los datos de prueba autorizados.

## Contratos publicados y limites

### Conexion

- El panel Inicio > Integraciones usa `GET /v1/kivly` para mostrar la location
  autorizada (nombre e ID) tras conectar. Cada par de credenciales autoriza una
  sola location, no todas las locations del propietario ni sus branches.
  El `locationId` se guarda en la conexion del organizador al conectar y
  `prepare_settings` lo incorpora a los juegos al activar su requisito Kivly.
- En la configuracion del juego se reutiliza el nombre del contexto Kivly si
  aun no hay nombre guardado. Los campos vacios se rellenan en la interfaz con
  ejemplos de demo (`example.com`), siempre con el registro desactivado.
  Estos enlaces no son politicas del negocio: tanto la interfaz como el backend
  impiden activar altas reales con ellos. Los datos existentes se conservan.
- Inicio > Configuracion > Integraciones centraliza los datos del club:
  `PUT /v1/kivly` con `clubSettings: {clubName, privacyUrl, termsUrl}` los guarda
  en la conexion del organizador. GET los devuelve en `connection.clubSettings`.
  No hay interruptores ni formularios Kivly por juego. Los juegos resuelven
  la conexion y el club de la cuenta en cada lectura de metadatos, tanto en
  el acceso publico como en la asignacion de jugadores. La desconexion global
  desactiva el requisito; un cambio legal genera una nueva version de consentimiento.
  Una conexion con datos legales validos habilita el requisito de STACK sin una
  lista manual de ubicaciones aprobadas. Los enlaces de demo no habilitan altas reales.
  En el menu del juego se muestra solamente el logo y el estado de conexion.
  Cambiar de location al reconectar descarta los datos del club anterior.
  Las conexiones nuevas guardan por defecto el nombre de la location y los
  enlaces de Stand `/pages/privacy-es.html` y `/pages/legal.html`. Este ultimo
  es el aviso legal existente, no unas condiciones especificas del club.
  La interfaz prerrellena esos valores si faltan o si habia enlaces de demo;
  los valores personalizados se conservan y se guardan con Guardar club.
- Base usada por el cliente local: `https://api.getkivly.com`.
- `POST /api/v1/auth/token` con credenciales de ubicacion. Token en `data.token`.
- `GET /api/v1/integration/context`: alcance y presupuestos efectivos.
- El cliente comprueba coherencia de ubicacion/propietario entre token y contexto;
  el servidor aun debe comprobar que corresponden al negocio esperado.
- Limites publicados por defecto: 120 solicitudes/minuto y 5000/hora por
  ubicacion, compartidos entre tokens y servidores; no son capacidad garantizada.
  Respetar `Retry-After` en 429. Consultar contexto para valores efectivos.

### Crear cliente

`POST /api/v1/customers/`, Bearer, respuesta documentada 201 con
`CustomerResponse` directamente, sin asumir un envoltorio `data`.

- Requiere `name` y `email`.
- Entre los opcionales estan `phone`, `source`, `external_id`, `tags` y datos
  de sucursales. Su disponibilidad no implica que debamos recopilar todos.
- La respuesta incluye `id`; `email` y `pass_url_google` admiten null.
- El esquema no documenta que este POST complete la adhesion al club, genere
  tarjeta o envie un email.
- No se ha encontrado en este contrato una garantia de unicidad de email o
  `external_id`, ni una garantia de idempotencia del POST.
- La referencia advierte expresamente contra reintentar POST/PATCH tras timeout
  sin comprobar el resultado. No inventar soporte de `Idempotency-Key`.

### Buscar clientes

`GET /api/v1/customers/search/{query}` busca por nombre, email o telefono y
documenta un solo `CustomerResponse` o null, ademas de 404. No publica un conjunto
de coincidencias ni una garantia de correspondencia exacta o unicidad.
No sirve por si solo para descartar fichas duplicadas.

`GET /api/v1/customers/` ofrece un listado paginado:

- `page` desde 1, por defecto 1.
- `limit` entre 1 y 1000, por defecto 100.
- `search` busca por nombre, email o telefono; no esta documentado como igualdad
  exacta de email.
- Tambien admite `branchId`, `is_active` e `include_balance`.
- `PaginatedCustomersResponse` contiene `data`, `total`, `page`, `limit`,
  `has_next` y `has_previous`; no extrapolar la paginacion de premios.

Para reutilizar una ficha se necesita comprobar el email exacto, el negocio y
que no haya otra coincidencia. Un recorrido incompleto o fallido no significa
"cliente inexistente". La documentacion no garantiza lectura inmediatamente
consistente tras una escritura ni exclusividad frente a otras integraciones.

### Consulta de fidelizacion

`GET /api/v1/loyalty/customer/{location_id}/{identifier}` consulta por
`external_id`, email o telefono, con fallback legacy por identificador personal.
Codificar el identificador como un unico segmento de URL.

La referencia advierte que cliente ausente y algunos fallos internos pueden
devolver HTTP 200, saldo cero y usuario vacio. Tambien puede devolver una forma
legacy. Un 200 no demuestra coincidencia ni permite concluir que deba crearse una
ficha. Esta consulta no registra al cliente ni canjea un premio.

## Limites de lo confirmado con Kivly

No se han confirmado como garantias generales del proveedor:

1. La prueba autorizada y la comprobacion del usuario en el panel confirman que
   el Customer creado aparece como miembro del club en ese negocio de pruebas.
   Falta confirmar si es una garantia general o depende de su configuracion y
   como se consulta la adhesion de una ficha existente.
2. Como recuperar con certeza una creacion tras timeout: unicidad por ubicacion,
   comportamiento ante email/external_id repetido, mecanismo idempotente si
   existe y consulta fiable del resultado. No basta con una busqueda vacia.
3. Como se detectan todas las coincidencias exactas por email, incluidos clientes
   inactivos, y que normalizacion aplica Kivly.

No son defectos demostrados del servicio: son garantias no publicadas en los
contratos revisados. No ejecutar altas reales para descubrirlas sin acordar una
prueba controlada. Una prueba aislada tampoco demuestra una garantia contractual.

La implementacion no presupone esas garantias: comprueba el alcance del negocio
conectado y conserva un intento de creacion durable, sin enviar
un segundo POST si su resultado es incierto. Una coincidencia ambigua, inactiva o
una busqueda incompleta bloquea el alta. Esto no garantiza exclusividad frente
a otras integraciones que creen fichas simultaneamente en Kivly.

## Prueba controlada en un negocio de pruebas

Se ha acordado probar con un negocio de pruebas sin clientes reales. El host
sigue siendo la API de produccion: **negocio de pruebas no significa servidor
staging**. No cambiar el host ni usar las credenciales del cliente real.
La herramienta no puede determinar si una ubicacion es realmente de pruebas:
el operador debe verificarlo en el panel antes de confirmar.

Desde la raiz del backend, en una terminal interactiva:

```powershell
.\.venv\Scripts\python.exe .\scripts\probe_kivly_registration.py
```

Si ese entorno virtual no existe, usar el Python del comprobador de conexion:

```powershell
python .\scripts\probe_kivly_registration.py
```

Pasos de la herramienta:

1. Solicitar de forma oculta el `location_id` esperado del negocio de pruebas,
   un email de pruebas controlado por el operador y las credenciales de ese
   negocio. No introducir datos de terceros ni pegarlos en chat o argumentos.
2. Autenticar, comprobar contexto y exigir que la ubicacion coincida con la
   indicada. Antes de cada operacion de clientes, comprobar tambien el alcance
   del token, incluso si se renueva.
3. Buscar el email en el listado paginado, sin filtro de activos. Comparar
   emails completos tras trim/casefold como criterio local de la prueba, no
   como afirmacion sobre la normalizacion interna de Kivly. Recorrer como
   maximo 5 paginas de 100; si el recorrido es incompleto o ambiguo, detenerse.
4. Si ya existe una ficha coincidente, no crear ni modificar nada.
5. Si no existe, explicar el efecto y exigir escribir
   `CREAR EN NEGOCIO DE PRUEBAS`. Crear como maximo una ficha con nombre
   `Stand integration test`, el email indicado, source `stand-controlled-test`
   y un external_id aleatorio `stand-probe-...`. Puede activar automatizaciones
   configuradas en ese negocio; por eso el email debe estar bajo tu control.
6. Consultar de nuevo y comprobar que se recupera el mismo ID. Mostrar solo
   indicadores, sin token, email, ID, nombre del negocio ni enlace de Wallet.
   La presencia de un enlace no prueba que funcione o que se haya enviado.

Antes del POST se reserva de forma exclusiva y persistente el archivo local
`.kivly-registration-probe.lock`, excluido de Git. Solo contiene el identificador
aleatorio del intento. Nunca se elimina automaticamente; evita un segundo intento
de creacion desde esa copia local incluso tras timeout, interrupcion o fallo de
lectura posterior. No garantiza exclusividad frente a otras copias o sistemas.

No borrar el archivo para reintentar un resultado incierto. Revisar primero el
panel y reconciliar el intento. No hay reintento automatico, borrado remoto ni
prueba de creacion duplicada. Para consultar sin posibilidad de crear:

```powershell
python .\scripts\probe_kivly_registration.py --lookup-only
```

Se admite `--ca-bundle "C:\ruta\ca-confiable.pem"` en ambos modos. TLS sigue
verificado y no se siguen redirecciones. No se escriben credenciales en disco.
Codigos de salida: 0, ficha encontrada/creada y consultada; 1, fallo que requiere
revision; 2, entrada no interactiva, cancelada o invalida.

**Resultado que necesitamos comprobar en el panel:** localizar por el email de
pruebas la ficha `Stand integration test` y comprobar si figura inscrita/activa
en el club, no solo como contacto. Compartir la conclusion sin capturas con
datos personales ni credenciales. Si el panel no distingue ambos conceptos,
solicitar confirmacion a Kivly. No se ha demostrado aun este resultado real.
Tampoco se prueban garantias generales de unicidad/idempotencia con esta prueba.

### Resultado real del 24 de octubre de 2026

El usuario autorizo crear una ficha en un negocio de pruebas y proporciono
configuracion local. No se incluyen aqui valores de configuracion o datos de
la ficha.

- La consulta con `search` devolvio cero filas, `total: 1` y `has_next: false`.
  El comprobador se detuvo correctamente ante esa discrepancia.
- Como diagnostico alternativo se consulto el listado **sin filtro**: una fila,
  `total: 1`, sin siguiente pagina y sin coincidencia exacta del email de pruebas.
  No se cambio ni relajo la validacion de `find_exact_email`.
- Se hizo un unico POST autorizado, reservando antes el marcador persistente.
  No hubo reintento de creacion.
- La lectura posterior sin filtro recupero una unica coincidencia del email,
  con el mismo ID devuelto al crear, `is_active: true` y el `external_id` del
  intento. Creacion y recuperacion de la ficha confirmadas.
- `pass_url_google` no contenia un enlace. No se comprobo envio de email.
- El usuario reviso la ficha en el panel y confirmo: "Aparece como miembro del
  club". Esto confirma el resultado observado en ese negocio de pruebas, no una
  garantia para otras configuraciones o para clientes existentes.
- No se habilito el registro Stand y no se desplego nada.

Esta ejecucion se hizo con un procedimiento local acotado a un listado completo
de una pagina, no mediante `main()` del comprobador interactivo, que sigue
rechazando la discrepancia de `search`. No generalizar ese procedimiento para
negocios con mas paginas ni considerar confirmada la semantica de `total` con
filtros. El marcador de intento queda conservado; no repetir la creacion.

Los casos utiles de las comprobaciones simuladas se condensan al final de esta
guia. Sus archivos de tests se han retirado; los scripts operativos se conservan.

## Alta dual implementada

### Configuracion del organizador y del juego

- `GET /v1/game/kivly?gameId=...`: solo el propietario del STACK. Devuelve
  `connection`, `settings` y `reward: null`, nunca credenciales. Si hay conexion,
  comprueba contexto, alcance y permiso de lectura de clientes. Un fallo no se
  convierte en configuracion desconectada por defecto.
- `PUT /v1/game/kivly`: `{gameId, clientId, clientSecret}`. Verifica conexion y
  lectura, crea un secreto de version unica en Secrets Manager bajo
  `stand/<env>/kivly/<hash-organizador>/<version>` y guarda solo su ARN, ubicacion,
  propietario Kivly y version en `users.kivlyConnection`. El guardado usa
  comparacion de version para no sobrescribir otra conexion concurrente.
- `DELETE /v1/game/kivly?gameId=...`: desconecta al organizador y afecta a todos
  sus juegos. No desactiva silenciosamente sus requisitos de registro.
- Los secretos sustituidos se programan para borrado con recuperacion de 7 dias.
  Si falla la limpieza, la respuesta exitosa incluye
  `warning: KivlySecretCleanupRequired`. No equivale a revocar claves en Kivly.
  Un guardado incierto no borra el secreto nuevo, que podria estar en uso;
  requiere comprobar el estado y revisar posibles secretos huerfanos.
- El cliente de asignacion y el de configuracion reutilizan como maximo ocho
  instancias en memoria por contenedor Lambda, separadas por ARN/version de
  credencial. Cada instancia conserva su propio token y verifica su alcance
  tambien al renovarlo. No hay cache de tokens compartida entre servidores.
- `PUT /v1/game` acepta `kivly: {enabled, clubName, privacyUrl, termsUrl}`.
  No mezclar `rewardId` dentro de ese objeto: el premio usa el PUT dedicado
  `/v1/game/kivly` descrito arriba. Todo registro web exige email verificado,
  con o sin Kivly; no cambia el flujo de entrada por Instagram.

### Activacion mediante la conexion del organizador

No hay lista manual de ubicaciones aprobadas ni parametro de despliegue para
autorizar cada negocio. La conexion del organizador y los datos legales validos
del club habilitan el requisito compartido en sus juegos. Una conexion existente
que estuviera bloqueada solo por la antigua lista quedara habilitada al actualizar
el backend. Los enlaces de demo siguen sin permitir altas reales.

Las respuestas de conexion indican `connected`, sin un indicador separado de
aprobacion de membresia. Conectado acredita conexion y lectura del negocio,
no el alta de un participante: esta se confirma durante el registro.
Se conserva la comprobacion de propietario/ubicacion y la vinculacion al email
verificado con consentimiento. Revisar los datos compartidos del club antes
de registrar participantes.

El cliente Kivly no permite demostrar anticipadamente permisos de escritura
sin escribir: la conexion comprueba autenticacion/contexto y lectura, no promete
que un futuro POST vaya a ser autorizado.

### Registro, vinculo y recuperacion

1. Play exige aceptar las condiciones y enviar `loyaltyConsentVersion` al pedir
   el codigo de email. El servidor calcula la version a partir del nombre del
   club, URLs y ubicacion. La version aceptada forma parte del registro ligado
   criptograficamente al codigo/recibo de verificacion.
2. Solo despues de verificar el email, el asignador comprueba la conexion del
   organizador y busca coincidencia exacta de email en el negocio autorizado.
   No modifica una ficha activa existente. Un cliente nuevo recibe nombre,
   email, source `stand`, external_id y telefono si se habilito y proporciono
   en el formulario verificado. No envia foto.
3. La busqueda recorre el listado **sin filtro**, validando paginas, totales e
   IDs unicos; compara emails completos tras trim/casefold. Maximo cinco paginas
   de 100 registros. Superar ese presupuesto, encontrar cambios de paginacion o
   varias coincidencias devuelve un error explicito, nunca "cliente inexistente".
   Esta solucion acotada es adecuada para la integracion inicial; negocios mayores
   requieren confirmar con Kivly un contrato de busqueda exacta fiable antes de
   ampliar su activacion. No se descarga ni registra el catalogo de clientes.
4. Una tabla DynamoDB dedicada, retenida y sin TTL, guarda un identificador HMAC
   por organizador/ubicacion/email, el estado y el ID del cliente. El email no
   aparece en la clave ni en los logs de la integracion. Antes de crear, se
   reserva condicionalmente un estado `creating` con external_id aleatorio.
   Solo el ganador puede enviar el POST.
5. Ante timeout, respuesta inesperada o fallo de persistencia, conservar el
   intento. Los reintentos consultan: solo recuperan una ficha activa con email
   exacto y el external_id del intento. Si no pueden confirmarla, devuelven
   `KivlyReconciliationRequired`; nunca vuelven a crear automaticamente.
6. Tras confirmar, persistir el ID Kivly y la version/fecha de consentimiento
   en `player.kivlyMembership`. Los siguientes juegos del mismo organizador y
   ubicacion reutilizan el vinculo confirmado, sin crear otra ficha. Ese vinculo
   acredita la confirmacion registrada, no una comprobacion continua de bajas
   posteriores en Kivly; no hay sincronizacion por webhooks en este incremento.
7. No entregar sesion util hasta confirmar. Recuperacion de sesion, puntuacion,
   foto y entrega de premios locales comprueban el requisito. Una sesion anterior
   sin membresia o con condiciones antiguas debe pasar de nuevo por el registro.
   Desconectar/cambiar de ubicacion bloquea los juegos que la siguen exigiendo.

Si se alcanza el limite de jugadores despues de confirmar Kivly, no se elimina
la ficha externa: las dos plataformas no comparten una transaccion atomica.
Un reintento reutiliza la confirmacion, sin repetir el alta.

### Operacion ante un resultado incierto

Mostrar el error del registro y no volver a crear desde el panel de Kivly ni borrar
el registro durable para "desbloquearlo". Revisar en el negocio autorizado la
ficha, el email y el external_id del intento mediante soporte autorizado.
Si no aparece, mantener el estado pendiente hasta aclarar con Kivly si la
operacion llego a realizarse. Este incremento no incluye una herramienta de
reconciliacion manual ni un boton de reenvio del POST.

No exponer cuerpos externos, tokens, claves o emails en logs/errores. 429 conserva
`retryAfter` para el formulario; errores de acceso o servicio se notifican sin
simular un alta correcta. No hay reintentos automaticos de escrituras.

### Despliegue de los cambios locales

Tras aprobar los PRs, desplegar primero el backend y su SAM: tabla de registros
retenida con PITR, permisos de secretos limitados a su prefijo y endpoints
autenticados de conexion. Despues publicar el frontend compatible. Verificar el
recorrido de registro STACK en desarrollo antes del evento. No desplegar sin
aprobacion expresa.

### Prueba manual desde el frontend local

Usar el backend de dev actualizado y un negocio de pruebas autorizado. Localhost
no simula Kivly: el registro crea una ficha real si el email no existe.

1. Desde `stand-frontend`, ejecutar
   `node .\node_modules\vite\bin\vite.js --host localhost`.
   Revisar que las variables del modo development apuntan a la API y cliente
   Cognito de dev; el sufijo `/prod` puede ser correcto para esa API de dev.
2. Iniciar sesion y abrir Configuracion > Integraciones. Comprobar que
   `GET /v1/kivly` devuelve `connection.connected: true` y datos del club validos.
   No se requiere aprobar la ubicacion por separado.
3. Abrir un STACK activo con acceso web/movil desde una ventana privada en
   `http://localhost:5173/play/ID_DEL_JUEGO`, adaptando el puerto a Vite.
   `GET /v1/public/game?gameId=...` debe incluir `loyalty` y el formulario debe
   pedir email y consentimiento del club.
4. Usar un email propio nuevo en ese negocio, aceptar privacidad y condiciones,
   solicitar el codigo y completarlo. La solicitud de codigo incluye
   `loyaltyConsentVersion`; el join envia `gameId`, `challengeId` y `code`.
   No compartir codigos, tokens ni respuestas con datos personales.
5. Comprobar el acceso a STACK y el participante en Stand, y buscar el mismo
   email en Kivly: debe haber una sola ficha como miembro del club.
   El navegador solo llama a Stand; Kivly se invoca desde el backend despues
   de verificar el email. No son dos altas paralelas ni una transaccion atomica.

Un error de entrega de email bloquea la prueba antes del alta. Un resultado
incierto de Kivly requiere revision, no una segunda creacion manual. Esta prueba
no acredita envio de tarjeta, Wallet ni recompensas.

## Puntos de entrada locales

Rutas relativas a cada repositorio:

| Repositorio | Archivo | Papel |
|-------------|---------|-------|
| Backend | `layers\stand_common\python\stand_common\kivly.py` | Token/contexto y listado/creacion de clientes con alcance validado |
| Backend | `layers\stand_common\python\stand_common\kivly_registration.py` | Consentimiento, estado durable y proteccion de acceso |
| Backend | `layers\stand_common\python\stand_common\kivly_settings.py` | Conexion del organizador y almacenamiento seguro |
| Backend | `scripts\check_kivly_connection.py` | Comprobacion local interactiva sin escrituras de negocio |
| Backend | `scripts\probe_kivly_registration.py` | Prueba local con creacion explicita de una ficha en negocio de pruebas |
| Backend | `src\stand_prod_game_fn_public\lambda_function.py` | Registro, metadatos de club y proteccion de sesion |
| Frontend | `src\api\kivlyApi.ts` | Solo contrato de conexion y ajustes de registro |
| Frontend | `src\core\settings\KivlySettings.tsx` | Conexion compartida y requisito de registro por juego |
| Frontend | `src\core\participant\JoinForm.tsx` | Consentimiento del club y mensajes del registro |
| Frontend | `src\core\participant\ParticipantShell.tsx` | Recuperacion de sesion |

## Verificacion del alta dual

Las pruebas actuales de cuenta/configuracion y frontend cubren el contrato sin
aprobacion manual, conexion y ubicacion, datos legales, consentimiento y
reutilizacion de membresia confirmada. Son pruebas simuladas, no evidencia
de entrega de email ni de un alta real desde Play. Las pruebas adicionales
cubren premio unico, estados inciertos y enlaces Wallet; no acreditan entrega
real del premio o tarjeta. No hay envio de tarjeta por email.

Lista condensada para futuras comprobaciones (no ejecutarla contra clientes
reales ni repetir escrituras inciertas):

| Caso | Resultado necesario |
|------|---------------------|
| Cliente nuevo / existente activo / inactivo | Crear una vez / reutilizar sin modificar / bloquear |
| Email sin verificar o consentimiento ausente/antiguo | No crear ni permitir jugar; pedir registro vigente |
| Paginacion completa, inconsistente o >500 registros; emails duplicados | Solo ausencia confirmada permite crear; dudas bloquean explicitamente |
| Credenciales, contexto o negocio incorrectos; 401/403/429 | Error sanitizado; respetar espera; nunca simular alta |
| Doble envio, timeout tras POST o fallo al persistir | Conservar intento; recuperar por email y external_id; nunca un segundo POST |
| Reconexion concurrente, guardado ambiguo y limpieza fallida | No sobrescribir version ajena ni borrar secreto posiblemente referenciado; avisar de limpieza |
| Desconexion o cambio de club con sesiones antiguas | Bloquear acceso y pedir registro vigente cuando proceda |
| Formulario / ajustes en ES y EN | Email obligatorio, consentimiento explicito versionado, guardado unico; sin claves en almacenamiento/logs |
| Juego sin Kivly | Conservar registro, puntuacion, fotos y premios locales |

Las comprobaciones locales habituales son la suite preexistente, tipos/build
del frontend y `sam validate --lint`. No sustituyen la comprobacion autorizada
del alta dual en un entorno desplegado.

Separar pruebas simuladas, comprobacion real controlada y despliegue. Ninguna
de ellas sustituye a las otras. No desplegar ni crear clientes reales de forma
automatica por haber aprobado este plan.

## Fuentes oficiales

- [Indice](https://kivly.readme.io/llms.txt)
- [Token](https://kivly.readme.io/reference/login_with_client_id_and_secret_api_v1_auth_token_post.md)
- [Contexto](https://kivly.readme.io/reference/integration_context_api_v1_integration_context_get.md)
- [Crear cliente](https://kivly.readme.io/reference/create_customer_api_v1_customers__post.md)
- [Buscar cliente](https://kivly.readme.io/reference/search_customer_api_v1_customers_search__query__get.md)
- [Listado paginado](https://kivly.readme.io/reference/get_customers_api_v1_customers__get.md)
- [Consulta de fidelizacion](https://kivly.readme.io/reference/getcustomerloyalty.md)
