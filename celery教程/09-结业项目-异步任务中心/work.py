from tasks import final_app_add, final_app_sub


def main():
    # final_app_add.delay(1, 2)
    final_app_add.apply_async(args=(3, 4))
    final_app_sub.apply_async(args=(10, 4))


if __name__ == "__main__":
    main()
