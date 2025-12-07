<p align="center">
  <img src="https://i.imgur.com/TzcVUYs.png" alt="Rac-delta logo" width="140"/>
  <img src="https://nodejs.org/static/logos/jsIconGreen.svg" alt="node logo" width="100"/>
</p>

---

# rac-delta 🦝

rac-delta is an open delta-patching protocol made for sync builds of your apps, games, or anything you store in a folder!

This is the official SDK for NodeJs.

## Benefits of rac-delta

- **Backend agnostic**: it does not depend on a concrete service (S3, Azure, SSH, signed URLs, etc.)
- **Modular**: almost any remote storage can be used via adapters
- **Simple**: an unique index archive (rdindex.json) centralices all information.
- **Efficiency**: supports streaming, concurrency, and uses **Blake3** for fast hashing.
- **Flexibility**: highly customizable, chunk size, concurrency limits, reconstruction strategies...

---

## Installation

```
npm install rac-delta
```

## Basic usage

In order to use the rac-delta SDK, you will need to create a RacDeltaClient, the main entry point of the SDK and where all the rac-delta operations are invoked.

```ts
import { RacDeltaClient } from 'rac-delta';

const racDeltaClient = new RacDeltaClient({
  chunkSize: 1024 * 1024,
  maxConcurrency: 6,
  storage: {
    type: 'ssh',
    host: 'localhost',
    pathPrefix: '/root/upload',
    port: 2222,
    credentials: {
      username: 'root',
      password: 'password',
    },
  },
});
```

And a example to perform a upload to the selected storage (SSH in this case)

```ts
const remoteIndexToUse = undefined;

await racDeltaClient.pipelines.upload.execute('path/to/build', remoteIndexToUse, {
  requireRemoteIndex: false,
  force: false,
  ignorePatterns: undefined,
  onStateChange: (state) => {
    console.log(state);
  },
  onProgress: (type, progress, speed) => {
    console.log(type, progress.toFixed(1), speed?.toFixed(1));
  },
});
```

## Documentation

For all the details, check the [full documentation](https://raccreative.github.io/rac-delta-docs/).

Available in spanish too!

---

## Contributing

Contributions are welcome!

1. Fork the repository.
2. Create a branch for your feature/fix (`git checkout -b feature/new-feature`).
3. Commit your changes (`git commit -m 'Added new feature'`).
4. Push to your branch (`git push origin feature/new-feature`).
5. Open a Pull Request.

## License

This project is licensed under the [MIT License](LICENSE).
