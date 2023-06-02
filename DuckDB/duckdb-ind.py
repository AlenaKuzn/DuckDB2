#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import duckdb
import typing as t
from pathlib import Path


def display_reys(re: t.List[t.Dict[str, t.Any]]) -> None:
    """
    Отобразить список рейсов.
    """
    # Проверить, что список рейсов не пуст.
    if re:
        line = '+-{}-+-{}-+-{}-+-{}-+'.format(
            '-' * 4,
            '-' * 30,
            '-' * 11,

            '-' * 20
        )
        print(line)
        print(
            '| {:^4} | {:^30} | {:^11} | {:^20} |'.format(
                "No",
                "Пункт назначения",
                "Номер рейса",
                "Тип"
            )
        )
        print(line)

        # Вывести данные о всех рейсах.
        for idx, rey in enumerate(re, 1):
            print(
                '| {:>4} | {:<30} | {:<11} | {:>20} |'.format(
                    idx,
                    rey.get('pynkt', ''),
                    rey.get('samolet', 0),
                    rey.get('numb', '')
                )
            )
            print(line)

    else:
        print("Список рейсов пуст.")


def create_db(database_path: Path) -> None:
    """
    Создать базу данных.
    """
    conn = duckdb.connect(str(database_path))
    cursor = conn.cursor()

    # Создать таблицу с информацией.
    cursor.execute(
        """
        CREATE SEQUENCE IF NOT EXISTS type_st START 1
        """
    )
    cursor.execute(
        """
        CREATE SEQUENCE IF NOT EXISTS plane_st START 1
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS types (
            type_id INTEGER PRIMARY KEY,
            type_title TEXT NOT NULL
        )
        """
    )

    # Создать таблицу с информацией о рейсах.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS reys (
            plane_id INTEGER PRIMARY KEY,
            plane_destination TEXT NOT NULL,
            type_id INTEGER NOT NULL,
            plane_num INTEGER NOT NULL,
            FOREIGN KEY(type_id) REFERENCES types(type_id)
        )
        """
    )

    conn.close()


def get_reys(
        database_path: Path,
        pynkt: str,
        numb: int,
        samolet: str
) -> None:
    """
    Добавить рейсы в базу данных.
    """
    conn = duckdb.connect(str(database_path))
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT type_id FROM types WHERE type_title = ?
        """,
        (samolet,)
    )
    row = cursor.fetchone()
    if row is None:
        cursor.execute(
            """
            INSERT INTO types VALUES (nextval('type_st'), ?)
            """,
            (samolet,)
        )
        cursor.execute(
            """
            SELECT currval('type_st')
            """
        )
        sel = cursor.fetchone()
        type_id = sel[0]

    else:
        type_id = row[0]

    # Добавить информацию о новом рейсе.
    cursor.execute(
        """
        INSERT INTO reys (plane_id, plane_destination, type_id, plane_num) 
        VALUES (nextval('plane_st'), ?, ?, ?)
        """,
        (pynkt, type_id, numb)
    )

    conn.commit()
    conn.close()


def select_all(database_path: Path) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать все рейсы.
    """
    conn = duckdb.connect(str(database_path))
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT reys.plane_destination, types.type_title, reys.plane_num
        FROM reys
        INNER JOIN types ON types.type_id = reys.type_id
        """
    )
    rows = cursor.fetchall()

    conn.close()
    return [
        {
            "pynkt": row[0],
            "numb": row[1],
            "samolet": row[2],
        }
        for row in rows
    ]


def select_by_pynkt(
        database_path: Path, jet: str
) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать самолеты с заданным пунктом.
    """

    conn = duckdb.connect(str(database_path))
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT reys.plane_destination, types.type_title, reys.plane_num
        FROM reys
        INNER JOIN types ON types.type_id = reys.type_id
        WHERE reys.plane_destination = ?
        """,
        (jet,)
    )
    rows = cursor.fetchall()

    conn.close()
    return [
        {
            "pynkt": row[0],
            "numb": row[1],
            "samolet": row[2],
        }
        for row in rows
    ]


def main(command_line=None):
    # Создать родительский парсер для определения имени файла.
    file_parser = argparse.ArgumentParser(add_help=False)
    file_parser.add_argument(
        "--db",
        action="store",
        required=False,
        default=str(Path.home() / "reys.db"),
        help="The database file name"
    )

    # Создать основной парсер командной строки.
    parser = argparse.ArgumentParser("reys")
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 0.1.0"
    )

    subparsers = parser.add_subparsers(dest="command")

    # Создать субпарсер для добавления рейса.
    add = subparsers.add_parser(
        "add",
        parents=[file_parser],
        help="Add a new worker"
    )
    add.add_argument(
        "-p",
        "--pynkt",
        action="store",
        required=True,
        help="The pynkt"
    )
    add.add_argument(
        "-n",
        "--numb",
        action="store",
        help="The number reys"
    )
    add.add_argument(
        "-s",
        "--samolet",
        action="store",
        required=True,
        help="The type samolet"
    )

    # Создать субпарсер для отображения всех рейсов.
    _ = subparsers.add_parser(
        "display",
        parents=[file_parser],
        help="Display all reys"
    )

    # Создать субпарсер для выбора рейсов.
    select = subparsers.add_parser(
        "select",
        parents=[file_parser],
        help="Select the reys"
    )
    select.add_argument(
        "-P",
        "--pynkt",
        action="store",
        required=True,
        help="The required pynkt"
    )

    # Выполнить разбор аргументов командной строки.
    args = parser.parse_args(command_line)

    # Получить путь к файлу базы данных.
    db_path = Path(args.db)
    create_db(db_path)

    # Добавить рейс.
    if args.command == "add":
        get_reys(db_path, args.pynkt, args.numb, args.samolet)

    # Отобразить все рейсы.
    elif args.command == "display":
        display_reys(select_all(db_path))

    # Выбрать требуемые рейсы.
    elif args.command == "select":
        display_reys(select_by_pynkt(db_path, args.pynkt))
        pass


if __name__ == "__main__":
    main()
