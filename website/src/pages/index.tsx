import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';
import HomepageFeatures from '@site/src/components/HomepageFeatures';

import styles from './index.module.css';

type Metric = {
  value: string;
  label: string;
};

const Metrics: Metric[] = [
  {value: '1,003 ev/s', label: 'sustained throughput, single node'},
  {value: '34 ms', label: 'webhook p50, publish to receipt'},
  {value: '59 ms', label: 'webhook p99 at full rate'},
  {value: '0 lost', label: 'across every injected fault'},
];

function Metric({value, label}: Metric) {
  return (
    <div className={styles.metric}>
      <div className={styles.metricValue}>{value}</div>
      <div className={styles.metricLabel}>{label}</div>
    </div>
  );
}

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <span className={styles.badge}>Alpha</span>
        <Heading as="h1" className="hero__title">
          {siteConfig.title}
        </Heading>
        <p className={clsx('hero__subtitle', styles.subtitle)}>
          {siteConfig.tagline}
        </p>
        <div className={styles.buttons}>
          <Link
            className="button button--secondary button--lg"
            to="/docs/getting-started">
            Get started
          </Link>
          <Link
            className="button button--outline button--secondary button--lg"
            to="/docs/intro">
            Read the overview
          </Link>
        </div>
      </div>
    </header>
  );
}

function MetricsSection() {
  return (
    <section className={styles.metricsSection}>
      <div className="container">
        <div className={styles.metrics}>
          {Metrics.map((props, idx) => (
            <Metric key={idx} {...props} />
          ))}
        </div>
        <p className={styles.metricsNote}>
          Measured by an independent oracle that recomputes expected deliveries
          itself rather than asking the platform. Single-node Kafka, PostgreSQL
          and ClickHouse on one 8-core laptop —{' '}
          <Link to="/docs/performance">read the caveats</Link> before quoting
          them.
        </p>
      </div>
    </section>
  );
}

function CtaSection() {
  return (
    <section className={styles.ctaSection}>
      <div className="container">
        <div className="text--center">
          <Heading as="h2">Run it and break it</Heading>
          <p className={styles.ctaText}>
            The chaos driver is committed rather than described: publish at 200
            events/s, kill a dispatcher, a router and a gateway mid-run, then
            let an independent oracle judge whether anything was lost. Five
            scenarios, all reporting{' '}
            <code>missing=0 unexpected=0</code>.
          </p>
          <div className={styles.buttons}>
            <Link
              className="button button--primary button--lg"
              to="/docs/operations#chaos-scenarios">
              Chaos scenarios
            </Link>
            <Link
              className="button button--secondary button--lg"
              to="/docs/spec">
              Specification
            </Link>
          </div>
        </div>
      </div>
    </section>
  );
}

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title="CloudEvents in, guaranteed delivery out"
      description="Eventarium accepts CloudEvents over REST, filters and projects them per subscription, and delivers over webhooks and SSE with tracked, at-least-once delivery.">
      <HomepageHeader />
      <main>
        <MetricsSection />
        <HomepageFeatures />
        <CtaSection />
      </main>
    </Layout>
  );
}
