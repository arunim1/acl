import { NextRequest, NextResponse } from 'next/server';
import { writeFile, mkdir } from 'fs/promises';
import { join } from 'path';
import { randomUUID } from 'crypto';

export async function POST(request: NextRequest) {
  try {
    const formData = await request.formData();
    const file = formData.get('file') as File;

    if (!file) {
      return NextResponse.json(
        { error: 'No file provided' },
        { status: 400 }
      );
    }

    // Validate file type
    if (!file.name.endsWith('.pgn')) {
      return NextResponse.json(
        { error: 'Invalid file type. Please upload a .pgn file' },
        { status: 400 }
      );
    }

    // Create unique session ID
    const sessionId = randomUUID();
    const uploadDir = join(process.cwd(), 'uploads', sessionId);

    // Create upload directory
    await mkdir(uploadDir, { recursive: true });

    // Read file content
    const bytes = await file.arrayBuffer();
    const buffer = Buffer.from(bytes);

    // Save file
    const filePath = join(uploadDir, 'input.pgn');
    await writeFile(filePath, buffer);

    return NextResponse.json({
      sessionId,
      fileName: file.name,
      fileSize: file.size,
      filePath,
    });
  } catch (error) {
    console.error('Upload error:', error);
    return NextResponse.json(
      { error: 'Failed to upload file' },
      { status: 500 }
    );
  }
}
