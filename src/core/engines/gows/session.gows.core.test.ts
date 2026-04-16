jest.mock('@adiwajshing/baileys', () => ({
  normalizeMessageContent: jest.fn(),
  proto: {},
}));

jest.mock('@waha/core/engines/noweb/session.noweb.core', () => ({
  extractBody: jest.fn(),
  getDestination: jest.fn(),
}));

import { WhatsappSessionGoWSCore } from '@waha/core/engines/gows/session.gows.core';
import { messages } from '@waha/core/engines/gows/grpc/gows';

function buildLogger() {
  return {
    child: jest.fn(() => buildLogger()),
    info: jest.fn(),
    debug: jest.fn(),
    error: jest.fn(),
    warn: jest.fn(),
    trace: jest.fn(),
  };
}

function buildSession() {
  const loggerBuilder = buildLogger();
  const session = new WhatsappSessionGoWSCore({
    name: 'default',
    printQR: false,
    loggerBuilder: loggerBuilder,
    sessionStore: {},
    mediaManager: {
      processMedia: jest.fn(),
      close: jest.fn(),
    },
    ignore: {
      status: false,
      groups: false,
      channels: false,
      broadcast: false,
    },
  });
  const sendMessage = jest.fn((request, callback) => {
    callback(null, {
      toObject: jest.fn(() => ({
        id: 'MESSAGE_ID',
        message: {
          data: JSON.stringify({
            conversation: 'sent',
          }),
        },
      })),
    });
  });
  (session as any).client = {
    SendMessage: sendMessage,
  };
  (session as any).me = {
    id: '11111111111@c.us',
  };
  return {
    session: session,
    sendMessage: sendMessage,
  };
}

describe('WhatsappSessionGoWSCore media sending', () => {
  it('sends a base64 image through GOWS media', async () => {
    const { session: session, sendMessage: sendMessage } = buildSession();
    const content = Buffer.from('image-content');

    const response = await session.sendImage({
      session: 'default',
      chatId: '22222222222',
      file: {
        mimetype: 'image/png',
        filename: 'picture.png',
        data: content.toString('base64'),
      },
      caption: 'image caption',
      reply_to: 'false_22222222222@c.us_REPLYID',
      mentions: ['33333333333'],
    });

    const request = sendMessage.mock.calls[0][0] as messages.MessageRequest;
    expect(request.jid).toBe('22222222222@s.whatsapp.net');
    expect(request.text).toBe('image caption');
    expect(request.replyTo).toBe('REPLYID');
    expect(request.mentions).toEqual(['33333333333@s.whatsapp.net']);
    expect(request.media.type).toBe(messages.MediaType.IMAGE);
    expect(request.media.mimetype).toBe('image/png');
    expect(request.media.filename).toBe('picture.png');
    expect(Buffer.from(request.media.content)).toEqual(content);
    expect(response).toEqual({
      id: 'true_22222222222@c.us_MESSAGE_ID',
      _data: {
        conversation: 'sent',
      },
    });
  });

  it('sends a base64 file through GOWS document media', async () => {
    const { session: session, sendMessage: sendMessage } = buildSession();
    const content = Buffer.from('file-content');

    await session.sendFile({
      session: 'default',
      chatId: '22222222222',
      file: {
        mimetype: 'application/pdf',
        filename: 'document.pdf',
        data: content.toString('base64'),
      },
      caption: 'file caption',
      reply_to: 'false_22222222222@c.us_REPLYID',
      mentions: ['33333333333'],
    });

    const request = sendMessage.mock.calls[0][0] as messages.MessageRequest;
    expect(request.jid).toBe('22222222222@s.whatsapp.net');
    expect(request.text).toBe('file caption');
    expect(request.replyTo).toBe('REPLYID');
    expect(request.mentions).toEqual(['33333333333@s.whatsapp.net']);
    expect(request.media.type).toBe(messages.MediaType.DOCUMENT);
    expect(request.media.mimetype).toBe('application/pdf');
    expect(request.media.filename).toBe('document.pdf');
    expect(Buffer.from(request.media.content)).toEqual(content);
  });

  it('fetches URL-backed files before sending media', async () => {
    const { session: session, sendMessage: sendMessage } = buildSession();
    const content = Buffer.from('remote-content');
    const fetch = jest.spyOn(session, 'fetch').mockResolvedValue(content);

    await session.sendFile({
      session: 'default',
      chatId: '22222222222',
      file: {
        mimetype: 'text/plain',
        filename: 'remote.txt',
        url: 'https://example.com/remote.txt',
      },
      caption: 'remote file',
    });

    const request = sendMessage.mock.calls[0][0] as messages.MessageRequest;
    expect(fetch).toHaveBeenCalledWith('https://example.com/remote.txt');
    expect(request.media.type).toBe(messages.MediaType.DOCUMENT);
    expect(Buffer.from(request.media.content)).toEqual(content);
  });
});
