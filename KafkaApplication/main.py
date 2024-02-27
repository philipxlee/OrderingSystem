from controller.kafka_controller import kafka_controller


def main():
    controller = kafka_controller()
    controller.run_program()


if __name__ == "__main__":
    main()
