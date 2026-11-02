NEON_READ_URL=""
NEON_DATABASE_URL=""
#NEON_READ_URL (etl_reader, solo lectura): se usa para copiar reservas F2 reales como duplicados. Si la dejas vacía, usa el archivo de ejemplo.
#NEON_DATABASE_URL (app_writer): reserva las filas de las agencias con reservar(). Si la dejas vacía, solo genera los archivos.

correrlo 
#docker compose up -d --build
#docker compose logs -f f4-weekly

sql para probar 

```sql
-- reservas de agencias por canal
SELECT c.code AS agencia, count(*) AS reservas
FROM operation.reservations r
JOIN operation.channels c ON c.id = r.channel_id
WHERE r.source = 'F4'
GROUP BY 1 ORDER BY 1;

-- rechazadas por falta de cupo
SELECT count(*) FROM operation.reservation_rejections WHERE source = 'F4';

-- comprobar que no hay sobreventa (debe dar 0)
SELECT count(*) FROM operation.inventory
WHERE sold_rooms > total_rooms + overbooking_limit;
```


esto me debe dar asi
#docker compose logs -f f4-weekly

```bash
PS C:\Users\57300\hospedaplus-csv> docker compose logs -f f4-weekly
f4-weekly-1  | [2026-11-02 16:45:35Z] 2026-W45: files already exist, not generating again
f4-weekly-1  | [2026-11-02 16:45:36Z] 2026-W45: validation PASSED_WITH_CATALOGUED_DEFECTS {'MISSING_CURRENCY': 21}
f4-weekly-1  | [2026-11-02 16:46:30Z] 2026-W45: Neon load {'confirmed': 183}
f4-weekly-1  | [2026-11-02 16:46:30Z] next run 2026-11-09 06:00Z
```