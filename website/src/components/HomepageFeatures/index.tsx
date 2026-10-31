import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type FeatureItem = {
  title: string;
  icon: string;
  description: ReactNode;
  link: string;
  linkLabel: string;
};

const FeatureList: FeatureItem[] = [
  {
    title: 'Filter and project per subscription',
    icon: '⌘',
    description: (
      <>
        CEL filters over event attributes and data, an allowlist projection,
        and an optional ordering key. Two subscribers of the same event type
        receive two different payloads — one narrowed, one filtered out
        entirely and recorded as such.
      </>
    ),
    link: '/docs/api',
    linkLabel: 'Subscription API',
  },
  {
    title: 'Webhooks and SSE',
    icon: '⇄',
    description: (
      <>
        Webhooks with HMAC signatures, an SSRF guard and per-endpoint rate
        limiting. SSE with heartbeats, explicit acks, and cursor reconnect
        that replays what you missed from the topic and merges it with the
        live stream.
      </>
    ),
    link: '/docs/api',
    linkLabel: 'Receiving deliveries',
  },
  {
    title: 'Tracked, guaranteed delivery',
    icon: '✓',
    description: (
      <>
        Eleven statuses from <code>pending</code> to <code>delivered</code> or{' '}
        <code>dead_lettered</code>, in an append-only store. Nothing vanishes:
        a delivery that cannot reach its receiver parks durably and drains
        when the receiver returns.
      </>
    ),
    link: '/docs/reliability',
    linkLabel: 'The eleven statuses',
  },
  {
    title: 'Failure handling that spares the receiver',
    icon: '⛨',
    description: (
      <>
        A per-subscription circuit breaker that costs a dead endpoint{' '}
        <strong>one request per probe interval</strong>, never a storm.
        <code>429</code> honours <code>Retry-After</code> without burning
        attempts; <code>410 Gone</code> is a clean, permanent opt-out.
      </>
    ),
    link: '/docs/reliability',
    linkLabel: 'Circuit breaker and dead letters',
  },
  {
    title: 'Transactional where it counts',
    icon: '⧉',
    description: (
      <>
        A publish batch lands in one Kafka transaction, so a half-failed batch
        is not partially accepted. The router wraps each poll batch —
        deliveries, pending statuses and the offset commit — in a single
        transaction.
      </>
    ),
    link: '/docs/architecture',
    linkLabel: 'Transactional boundaries',
  },
  {
    title: 'Scales out horizontally',
    icon: '⇶',
    description: (
      <>
        Any number of replicas of every service. Kafka partitions do the
        work-splitting; the two pieces it cannot partition are handled
        explicitly — an inbox lease electing one drainer, and a groupless
        presence view so every gateway sees every node's connections.
      </>
    ),
    link: '/docs/architecture',
    linkLabel: 'More than one replica',
  },
];

function Feature({title, icon, description, link, linkLabel}: FeatureItem) {
  return (
    <div className={clsx('col col--4', styles.featureCol)}>
      <div className={styles.featureCard}>
        <div className={styles.featureIcon} aria-hidden="true">
          {icon}
        </div>
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
        <Link className={styles.featureLink} to={link}>
          {linkLabel} →
        </Link>
      </div>
    </div>
  );
}

export default function HomepageFeatures(): ReactNode {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="text--center margin-bottom--lg">
          <Heading as="h2">What it does</Heading>
          <p className={styles.featuresIntro}>
            CloudEvents go in over REST. Each subscriber gets the events it
            asked for — filtered, projected, delivered, and tracked to a
            terminal outcome.
          </p>
        </div>
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
