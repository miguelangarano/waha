import {
  Inject,
  Injectable,
  NotFoundException,
  OnModuleInit,
  UnprocessableEntityException,
} from '@nestjs/common';
import {
  AppsService,
  IAppsService,
} from '@waha/apps/app_sdk/services/IAppsService';
import { EngineBootstrap } from '@waha/core/abc/EngineBootstrap';
import { GowsEngineConfigService } from '@waha/core/config/GowsEngineConfigService';
import { WPPEngineConfigService } from '@waha/core/config/WPPEngineConfigService';
import { WebJSEngineConfigService } from '@waha/core/config/WebJSEngineConfigService';
import { WhatsappSessionGoWSCore } from '@waha/core/engines/gows/session.gows.core';
import { WebhookConductor } from '@waha/core/integrations/webhooks/WebhookConductor';
import { MediaStorageFactory } from '@waha/core/media/MediaStorageFactory';
import { DefaultMap } from '@waha/utils/DefaultMap';
import { getPinoLogLevel, LoggerBuilder } from '@waha/utils/logging';
import { promiseTimeout, sleep } from '@waha/utils/promiseTimeout';
import { complete } from '@waha/utils/reactive/complete';
import { SwitchObservable } from '@waha/utils/reactive/SwitchObservable';
import { PinoLogger } from 'nestjs-pino';
import { EMPTY, merge, Observable, retry, share } from 'rxjs';
import { map } from 'rxjs/operators';

import { getNamespace, getSessionNamespace } from '../config';
import { WhatsappConfigService } from '../config.service';
import {
  WAHAEngine,
  WAHAEvents,
  WAHASessionStatus,
} from '../structures/enums.dto';
import {
  ProxyConfig,
  SessionConfig,
  SessionDetailedInfo,
  SessionDTO,
  SessionInfo,
} from '../structures/sessions.dto';
import { WebhookConfig } from '../structures/webhooks.config.dto';
import { populateSessionInfo, SessionManager } from './abc/manager.abc';
import { SessionParams, WhatsappSession } from './abc/session.abc';
import { EngineConfigService } from './config/EngineConfigService';
import { WhatsappSessionNoWebCore } from './engines/noweb/session.noweb.core';
import { WhatsappSessionWPPCore } from './engines/wpp/session.wpp.core';
import { WhatsappSessionWebJSCore } from './engines/webjs/session.webjs.core';
import { getProxyConfig } from './helpers.proxy';
import { MediaManager } from './media/MediaManager';
import { LocalSessionAuthRepository } from './storage/LocalSessionAuthRepository';
import { LocalStoreCore } from './storage/LocalStoreCore';
import { CoreApiKeyRepository } from './storage/CoreApiKeyRepository';
import { LocalSessionConfigRepository } from './storage/LocalSessionConfigRepository';

@Injectable()
export class SessionManagerCore extends SessionManager implements OnModuleInit {
  SESSION_STOP_TIMEOUT = 3000;

  private sessions: Map<string, WhatsappSession>;
  DEFAULT = 'default';

  protected readonly EngineClass: typeof WhatsappSession;
  protected events2: DefaultMap<
    string,
    DefaultMap<WAHAEvents, SwitchObservable<any>>
  >;
  protected wildcardEvents2: DefaultMap<WAHAEvents, SwitchObservable<any>>;
  protected readonly engineBootstrap: EngineBootstrap;

  constructor(
    config: WhatsappConfigService,
    private engineConfigService: EngineConfigService,
    private webjsEngineConfigService: WebJSEngineConfigService,
    private wppEngineConfigService: WPPEngineConfigService,
    gowsConfigService: GowsEngineConfigService,
    log: PinoLogger,
    private mediaStorageFactory: MediaStorageFactory,
    @Inject(AppsService)
    appsService: IAppsService,
  ) {
    super(log, config, gowsConfigService, appsService);
    this.sessions = new Map();
    const engineName = this.engineConfigService.getDefaultEngineName();
    this.EngineClass = this.getEngine(engineName);
    this.engineBootstrap = this.getEngineBootstrap(engineName);

    this.events2 = new DefaultMap<
      string,
      DefaultMap<WAHAEvents, SwitchObservable<any>>
    >(() => this.buildEventStreams());
    this.wildcardEvents2 = this.buildEventStreams();

    this.store = new LocalStoreCore(getNamespace(), getSessionNamespace());
    this.sessionAuthRepository = new LocalSessionAuthRepository(this.store);
    this.sessionConfigRepository = new LocalSessionConfigRepository(
      this.store,
    );

    this.clearStorage().catch((error) => {
      this.log.error({ error }, 'Error while clearing storage');
    });
  }

  protected getEngine(engine: WAHAEngine): typeof WhatsappSession {
    if (engine === WAHAEngine.WEBJS) {
      return WhatsappSessionWebJSCore;
    } else if (engine === WAHAEngine.WPP) {
      return WhatsappSessionWPPCore;
    } else if (engine === WAHAEngine.NOWEB) {
      return WhatsappSessionNoWebCore;
    } else if (engine === WAHAEngine.GOWS) {
      return WhatsappSessionGoWSCore;
    } else {
      throw new NotFoundException(`Unknown whatsapp engine '${engine}'.`);
    }
  }

  private buildEventStreams(): DefaultMap<WAHAEvents, SwitchObservable<any>> {
    return new DefaultMap<WAHAEvents, SwitchObservable<any>>(
      () =>
        new SwitchObservable((obs$) => {
          return obs$.pipe(retry(), share());
        }),
    );
  }

  private clearSessionEvents(name: string) {
    const events = this.events2.get(name);
    for (const eventName in WAHAEvents) {
      const event = WAHAEvents[eventName];
      events.get(event).switch(EMPTY);
    }
  }

  private updateWildcardEvents() {
    for (const eventName in WAHAEvents) {
      const event = WAHAEvents[eventName];
      const streams = Array.from(this.sessions.keys()).map((name) =>
        this.events2.get(name).get(event),
      );
      const stream$ = streams.length > 0 ? merge(...streams) : EMPTY;
      this.wildcardEvents2.get(event).switch(stream$);
    }
  }

  private getRunningSessions(): Record<string, WhatsappSession> {
    const sessions: Record<string, WhatsappSession> = {};
    for (const [name, session] of this.sessions.entries()) {
      sessions[name] = session;
    }
    return sessions;
  }

  async beforeApplicationShutdown(signal?: string) {
    for (const name of Array.from(this.sessions.keys())) {
      await this.stop(name, true);
    }
    this.stopEvents();
    await this.engineBootstrap.shutdown();
  }

  async onApplicationBootstrap() {
    this.apiKeyRepository = new CoreApiKeyRepository();
    await this.engineBootstrap.bootstrap();
    this.startPredefinedSessions();
  }

  private async clearStorage() {
    const storage = await this.mediaStorageFactory.build(
      'all',
      this.log.logger.child({ name: 'Storage' }),
    );
    await storage.purge();
  }

  //
  // API Methods
  //
  async exists(name: string): Promise<boolean> {
    return (
      this.sessions.has(name) ||
      (await this.sessionConfigRepository.exists(name))
    );
  }

  isRunning(name: string): boolean {
    return this.sessions.has(name);
  }

  async upsert(name: string, config?: SessionConfig): Promise<void> {
    await this.sessionConfigRepository.saveConfig(name, config);
  }

  async start(name: string): Promise<SessionDTO> {
    if (this.sessions.has(name)) {
      throw new UnprocessableEntityException(
        `Session '${name}' is already started.`,
      );
    }
    const savedConfig =
      (await this.sessionConfigRepository.getConfig(name)) ?? undefined;
    this.log.info({ session: name }, `Starting session...`);
    const logger = this.log.logger.child({ session: name });
    logger.level = getPinoLogLevel(savedConfig?.debug);
    const loggerBuilder: LoggerBuilder = logger;

    const storage = await this.mediaStorageFactory.build(
      name,
      loggerBuilder.child({ name: 'Storage' }),
    );
    await storage.init();
    const mediaManager = new MediaManager(
      storage,
      this.config.mimetypes,
      loggerBuilder.child({ name: 'MediaManager' }),
    );

    const webhook = new WebhookConductor(loggerBuilder);
    const proxyConfig = this.getProxyConfig(name, savedConfig);
    const sessionConfig: SessionParams = {
      name,
      mediaManager,
      loggerBuilder,
      printQR: this.engineConfigService.shouldPrintQR,
      sessionStore: this.store,
      proxyConfig: proxyConfig,
      sessionConfig: savedConfig,
      ignore: this.ignoreChatsConfig(savedConfig),
    };
    if (this.EngineClass === WhatsappSessionWebJSCore) {
      sessionConfig.engineConfig = this.webjsEngineConfigService.getConfig();
    } else if (this.EngineClass === WhatsappSessionWPPCore) {
      sessionConfig.engineConfig = this.wppEngineConfigService.getConfig();
    } else if (this.EngineClass === WhatsappSessionGoWSCore) {
      sessionConfig.engineConfig = this.gowsConfigService.getConfig();
    }
    await this.sessionAuthRepository.init(name);
    // @ts-ignore
    const session = new this.EngineClass(sessionConfig);
    this.sessions.set(name, session);
    this.updateSession(name, session);

    // configure webhooks
    const webhooks = this.getWebhooks(savedConfig);
    webhook.configure(session, webhooks);

    // Apps
    try {
      await this.appsService.beforeSessionStart(session, this.store);
    } catch (e) {
      logger.error(`Apps Error: ${e}`);
      session.status = WAHASessionStatus.FAILED;
    }

    // start session
    if (session.status !== WAHASessionStatus.FAILED) {
      await session.start();
      logger.info('Session has been started.');
      // Apps
      await this.appsService.afterSessionStart(session, this.store);
    }

    // Apps
    await this.appsService.afterSessionStart(session, this.store);

    return {
      name: session.name,
      status: session.status,
      config: session.sessionConfig,
    };
  }

  private updateSession(name: string, session: WhatsappSession) {
    for (const eventName in WAHAEvents) {
      const event = WAHAEvents[eventName];
      const stream$ = session
        .getEventObservable(event)
        .pipe(map(populateSessionInfo(event, session)));
      this.events2.get(name).get(event).switch(stream$);
    }
    this.updateWildcardEvents();
  }

  getSessionEvent(session: string, event: WAHAEvents): Observable<any> {
    if (session === '*') {
      return this.wildcardEvents2.get(event);
    }
    return this.events2.get(session).get(event);
  }

  async stop(name: string, silent: boolean): Promise<void> {
    if (!this.isRunning(name)) {
      this.log.debug({ session: name }, `Session is not running.`);
      return;
    }

    this.log.info({ session: name }, `Stopping session...`);
    try {
      const session = this.getSession(name);
      await session.stop();
    } catch (err) {
      this.log.warn(`Error while stopping session '${name}'`);
      if (!silent) {
        throw err;
      }
    }
    this.log.info({ session: name }, `Session has been stopped.`);
    this.sessions.delete(name);
    this.clearSessionEvents(name);
    this.updateWildcardEvents();
    await sleep(this.SESSION_STOP_TIMEOUT);
  }

  async unpair(name: string) {
    const session = this.sessions.get(name);
    if (!session) {
      return;
    }

    this.log.info({ session: name }, 'Unpairing the device from account...');
    await session.unpair().catch((err) => {
      this.log.warn(`Error while unpairing from device: ${err}`);
    });
    await sleep(1000);
  }

  async logout(name: string): Promise<void> {
    await this.sessionAuthRepository.clean(name);
  }

  async delete(name: string): Promise<void> {
    await this.appsService.removeBySession(this, name);
    this.sessions.delete(name);
    this.clearSessionEvents(name);
    this.updateWildcardEvents();
    await this.sessionConfigRepository.deleteConfig(name);
  }

  /**
   * Combine per session and global webhooks
   */
  private getWebhooks(sessionConfig?: SessionConfig) {
    let webhooks: WebhookConfig[] = [];
    if (sessionConfig?.webhooks) {
      webhooks = webhooks.concat(sessionConfig.webhooks);
    }
    const globalWebhookConfig = this.config.getWebhookConfig();
    if (globalWebhookConfig) {
      webhooks.push(globalWebhookConfig);
    }
    return webhooks;
  }

  /**
   * Get either session's or global proxy if defined
   */
  protected getProxyConfig(
    name: string,
    sessionConfig?: SessionConfig,
  ): ProxyConfig | undefined {
    if (sessionConfig?.proxy) {
      return sessionConfig.proxy;
    }
    const sessions = this.getRunningSessions();
    return getProxyConfig(this.config, sessions, name);
  }

  getSession(name: string): WhatsappSession {
    const session = this.sessions.get(name);
    if (!session) {
      throw new NotFoundException(
        `We didn't find a session with name '${name}'.\n` +
          `Please start it first by using POST /api/sessions/${name}/start request`,
      );
    }
    return session as WhatsappSession;
  }

  async getSessions(all: boolean): Promise<SessionInfo[]> {
    const names = all
      ? await this.sessionConfigRepository.getAllConfigs()
      : Array.from(this.sessions.keys());
    const result: SessionInfo[] = [];
    const uniqueNames = new Set([...names, ...this.sessions.keys()]);
    for (const name of uniqueNames) {
      const session = this.sessions.get(name);
      if (session) {
        const me = session.getSessionMeInfo();
        result.push({
          name: session.name,
          status: session.status,
          config: session.sessionConfig,
          me: me,
          presence: session.presence,
          timestamps: {
            activity: session.getLastActivityTimestamp(),
          },
        });
        continue;
      }
      if (!all) {
        continue;
      }
      const config = await this.sessionConfigRepository.getConfig(name);
      result.push({
        name: name,
        status: WAHASessionStatus.STOPPED,
        config: config,
        me: null,
        presence: null,
        timestamps: {
          activity: null,
        },
      });
    }
    return result;
  }

  private async fetchEngineInfo(name: string) {
    const session = this.sessions.get(name);
    // Get engine info
    let engineInfo = {};
    if (session) {
      try {
        engineInfo = await promiseTimeout(1000, session.getEngineInfo());
      } catch (error) {
        this.log.debug(
          { session: session.name, error: `${error}` },
          'Can not get engine info',
        );
      }
    }
    const engine = {
      engine: session?.engine,
      ...engineInfo,
    };
    return engine;
  }

  async getSessionInfo(name: string): Promise<SessionDetailedInfo | null> {
    const sessions = await this.getSessions(true);
    const session = sessions.find((item) => item.name === name);
    if (!session) {
      return null;
    }
    const engine = await this.fetchEngineInfo(name);
    return {
      ...session,
      engine: engine,
    };
  }

  protected stopEvents() {
    for (const events of this.events2.values()) {
      complete(events);
    }
    complete(this.wildcardEvents2);
  }

  async onModuleInit() {
    await this.init();
  }

  async init() {
    await this.store.init();
    await this.sessionConfigRepository.init();
    const knex = this.store.getWAHADatabase();
    await this.appsService.migrate(knex);
  }
}
