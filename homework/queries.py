"""Taller evaluable"""

# pylint: disable=broad-exception-raised
# pylint: disable=import-error


#
# ORQUESTADOR:
#
def run():
    """Orquestador.

    Lee `files/input/tips.csv` y genera los resultados de 5 consultas en:

    - `files/query_1/`
    - `files/query_2/`
    - `files/query_3/`
    - `files/query_4/`
    - `files/query_5/`

    Cada directorio contiene `_SUCCESS` y `part-00000`.
    """

    import csv
    import os
    from collections import defaultdict
    from collections.abc import Callable, Iterable, Iterator

    input_path = os.path.join("files", "input", "tips.csv")

    if not os.path.exists(input_path):
        raise Exception(f"Input file not found: {input_path}")

    with open(input_path, "r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    # Normalización básica de tipos numéricos.
    for row in rows:
        row["total_bill"] = float(row["total_bill"])
        row["tip"] = float(row["tip"])
        row["size"] = int(float(row["size"]))

    def map_reduce(
        data: Iterable[dict],
        mapper: Callable[[dict], Iterator[tuple[object, object]]],
        reducer: Callable[[object, list[object]], tuple[object, object] | None],
    ) -> list[tuple[object, object]]:
        """Ejecuta un pipeline MapReduce en memoria.

        - Map: emite pares (key, value)
        - Shuffle: agrupa values por key
        - Reduce: calcula un resultado por key
        """

        grouped: dict[object, list[object]] = defaultdict(list)
        for item in data:
            for key, value in mapper(item):
                grouped[key].append(value)

        results: list[tuple[object, object]] = []
        for key in sorted(grouped, key=lambda k: str(k)):
            reduced = reducer(key, grouped[key])
            if reduced is not None:
                results.append(reduced)
        return results

    def write_output(query_number: int, lines: list[str]) -> None:
        directory = os.path.join("files", f"query_{query_number}")
        os.makedirs(directory, exist_ok=True)
        success_path = os.path.join(directory, "_SUCCESS")
        part_path = os.path.join(directory, "part-00000")

        with open(success_path, "w", encoding="utf-8", newline=""):
            pass
        with open(part_path, "w", encoding="utf-8", newline="") as out:
            out.write("\n".join(lines))
            if lines:
                out.write("\n")

    # Query 1: Conteo de registros por día.
    def q1_mapper(row: dict) -> Iterator[tuple[str, int]]:
        yield (row["day"], 1)

    def q1_reducer(day: str, values: list[object]) -> tuple[str, int]:
        return (day, int(sum(values)))

    q1 = map_reduce(rows, q1_mapper, q1_reducer)
    q1_lines = [f"{day}\t{count}" for day, count in q1]
    write_output(1, q1_lines)

    # Query 2: Promedio de propina por sexo.
    def q2_mapper(row: dict) -> Iterator[tuple[str, tuple[float, int]]]:
        yield (row["sex"], (row["tip"], 1))

    def q2_reducer(sex: str, values: list[object]) -> tuple[str, float]:
        tip_sum = 0.0
        count = 0
        for tip, one in values:  # type: ignore[misc]
            tip_sum += float(tip)
            count += int(one)
        return (sex, tip_sum / count)

    q2 = map_reduce(rows, q2_mapper, q2_reducer)
    q2_lines = [f"{sex}\t{avg_tip:.6f}" for sex, avg_tip in q2]
    write_output(2, q2_lines)

    # Query 3: Suma de propina por fumador.
    def q3_mapper(row: dict) -> Iterator[tuple[str, float]]:
        yield (row["smoker"], row["tip"])

    def q3_reducer(smoker: str, values: list[object]) -> tuple[str, float]:
        return (smoker, float(sum(values)))

    q3 = map_reduce(rows, q3_mapper, q3_reducer)
    q3_lines = [f"{smoker}\t{tip_sum:.6f}" for smoker, tip_sum in q3]
    write_output(3, q3_lines)

    # Query 4: Promedio de total_bill por (día, tiempo).
    def q4_mapper(row: dict) -> Iterator[tuple[tuple[str, str], tuple[float, int]]]:
        yield ((row["day"], row["time"]), (row["total_bill"], 1))

    def q4_reducer(key: tuple[str, str], values: list[object]) -> tuple[tuple[str, str], float]:
        bill_sum = 0.0
        count = 0
        for bill, one in values:  # type: ignore[misc]
            bill_sum += float(bill)
            count += int(one)
        return (key, bill_sum / count)

    q4 = map_reduce(rows, q4_mapper, q4_reducer)
    q4_lines = [f"{day}\t{time}\t{avg_bill:.6f}" for (day, time), avg_bill in q4]
    write_output(4, q4_lines)

    # Query 5: Máxima propina por tamaño de mesa.
    def q5_mapper(row: dict) -> Iterator[tuple[int, float]]:
        yield (row["size"], row["tip"])

    def q5_reducer(size: int, values: list[object]) -> tuple[int, float]:
        return (size, float(max(values)))

    q5 = map_reduce(rows, q5_mapper, q5_reducer)
    q5_lines = [f"{size}\t{max_tip:.6f}" for size, max_tip in q5]
    write_output(5, q5_lines)


if __name__ == "__main__":

    run()
