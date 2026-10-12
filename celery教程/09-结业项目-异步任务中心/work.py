from tasks import final_app_add


def main():
    final_app_add.delay(1, 2)


if __name__ == "__main__":
    main()
