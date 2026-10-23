# OrderPlacementHappyPath
A system that only simulates the happy path of the backend logic for a e-commerce app. Using event based architecture and rabbit MQ.

## Execution

set up repo:
```
git clone https://github.com/InakiMedina/OrderPlacementHappyPath.git
cd OrderPlacementHappyPath
```

start docker and run order:
```
docker compose up -d
```

loggin:
```
docker compose logs -f --timestamps
```

## Architecture

### Broker topology vs Mediator Topology
the exact arquitectures wasn't specificied. However the diagrams show a chorographed architecture. Since this isn't a real business there isn't alot to work on while making this desicions. So it comes down to the fact that Mediator Topology is for complex architectures so the correct one should be:

**Broker Topology**

### Kafka vs Rabbit MQ
Since this is my first time using any of the two, Rabbit MQ is easier to use and there is no real advantage of the complexity of Kafka for a simple happy path that rquires no logs or rollbacks.


**Rabbit MQ**

## Folder Structure

└── my-project/
    ├── src/
    │   ├── components/
    │   │   ├── Button.tsx
    │   │   └── Card.tsx
    │   ├── pages/
    │   │   ├── index.tsx
    │   │   └── about.tsx
    │   └── app.tsx
    ├── public/
    │   └── favicon.ico
    ├── package.json
    └── README.md

## AI Usage

Gemini helped with the following
- troubleshooting docker
- helped me realized i don't need flask, just pika
- help me learn docker
- help me learn rabbit_mq
- help deciding logging option
