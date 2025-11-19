# Centipawn Loss Analyzer - Deployment Guide

## Quick Start for Vercel Deployment

### Prerequisites
- GitHub account
- Vercel account (free tier works great)
- Git repository pushed to GitHub

### Steps

1. **Prepare Repository**
   ```bash
   # Ensure all changes are committed
   cd /home/user/acl
   git add -A
   git commit -m "Ready for deployment"
   git push origin main  # or your branch name
   ```

2. **Deploy to Vercel**
   - Go to [vercel.com](https://vercel.com)
   - Click "New Project"
   - Import your GitHub repository
   - Select the `web` directory as the root
   - Click "Deploy"

3. **Configuration**
   - Framework Preset: Next.js
   - Root Directory: `web`
   - Build Command: `npm run build` (auto-detected)
   - Output Directory: `.next` (auto-detected)
   - Install Command: `npm install` (auto-detected)

4. **Environment Variables** (if needed)
   - None required for basic deployment
   - All processing is client-side

### Deployment Settings

```json
{
  "framework": "nextjs",
  "buildCommand": "npm run build",
  "devCommand": "npm run dev",
  "installCommand": "npm install",
  "outputDirectory": ".next"
}
```

## Alternative Deployment Options

### Netlify

1. Create `netlify.toml` in web directory:
```toml
[build]
  base = "web"
  command = "npm run build"
  publish = ".next"

[[plugins]]
  package = "@netlify/plugin-nextjs"
```

2. Connect repository to Netlify
3. Deploy

### Docker

1. Create `Dockerfile` in web directory:
```dockerfile
FROM node:18-alpine AS base

FROM base AS deps
RUN apk add --no-cache libc6-compat
WORKDIR /app
COPY package*.json ./
RUN npm ci

FROM base AS builder
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY . .
RUN npm run build

FROM base AS runner
WORKDIR /app
ENV NODE_ENV production
RUN addgroup --system --gid 1001 nodejs
RUN adduser --system --uid 1001 nextjs
COPY --from=builder /app/public ./public
COPY --from=builder --chown=nextjs:nodejs /app/.next/standalone ./
COPY --from=builder --chown=nextjs:nodejs /app/.next/static ./.next/static
USER nextjs
EXPOSE 3000
ENV PORT 3000
CMD ["node", "server.js"]
```

2. Build and run:
```bash
docker build -t chess-analyzer .
docker run -p 3000:3000 chess-analyzer
```

### AWS Amplify

1. Connect GitHub repository
2. Set build settings:
   - Build command: `npm run build`
   - Base directory: `web`
   - Output directory: `.next`
3. Deploy

### Self-Hosted (Node.js)

```bash
cd web
npm install
npm run build
npm start
```

Run on port 3000 by default. Use a reverse proxy (nginx) for production.

## Performance Optimization

### Before Deployment

1. **Bundle Analysis**
   ```bash
   npm run build
   # Check bundle size in output
   ```

2. **Image Optimization**
   - Use Next.js Image component
   - Serve from CDN if possible

3. **Caching**
   - Static assets cached automatically
   - Set cache headers in next.config.js

### After Deployment

1. **Monitor Performance**
   - Use Vercel Analytics (if on Vercel)
   - Monitor Core Web Vitals
   - Check error logs

2. **Optimize Load Time**
   - Enable compression (automatic on Vercel)
   - Use CDN for static assets
   - Lazy load components

## Security

### Client-Side Only
- No backend API endpoints
- All data stays in browser
- No database required
- No authentication needed

### Best Practices
- Always use HTTPS (automatic on Vercel)
- Set security headers in next.config.js
- Keep dependencies updated

## Monitoring

### Key Metrics to Watch
- First Contentful Paint (target: < 1.5s)
- Time to Interactive (target: < 3s)
- Processing time (target: < 5s per 1000 games)
- Error rate (target: < 0.1%)

### Tools
- Vercel Analytics
- Google Lighthouse
- WebPageTest
- Sentry (for error tracking)

## Troubleshooting

### Build Fails
1. Check Node.js version (18+)
2. Clear npm cache: `npm cache clean --force`
3. Delete node_modules and reinstall
4. Check for TypeScript errors

### Performance Issues
1. Check bundle size
2. Enable production mode
3. Use compression
4. Optimize images

### Parse Errors
1. Validate PGN format
2. Check for required annotations
3. Test with sample.pgn

## Updating

```bash
# Pull latest changes
git pull origin main

# Update dependencies
npm install

# Rebuild
npm run build

# Deploy
# Vercel: automatic on push
# Manual: npm start
```

## Scaling

### Current Limits
- File size: Browser memory (~100MB practical limit)
- Games processed: ~10,000 games efficiently
- Concurrent users: Unlimited (client-side)

### For Larger Datasets
1. Implement server-side processing
2. Use database for storage
3. Add pagination
4. Implement Web Workers for heavy computation

## Support

### Deployment Issues
- Check Vercel/platform status
- Review build logs
- Test locally first

### Runtime Issues
- Check browser console
- Verify file format
- Test with sample data

## Costs

### Free Tier (Vercel)
- Bandwidth: 100GB/month
- Builds: 6000 minutes/month
- Serverless executions: 100GB-hours
- **Perfect for this app!**

### Paid Plans (if needed)
- Pro: $20/month (unlimited builds)
- Enterprise: Custom pricing

## Checklist

Before deploying:
- [ ] All tests passing
- [ ] Build succeeds locally
- [ ] README updated
- [ ] Sample data included
- [ ] Error handling tested
- [ ] Performance optimized
- [ ] Mobile responsive
- [ ] Browser tested (Chrome, Firefox, Safari)
- [ ] Accessibility checked
- [ ] Documentation complete

After deploying:
- [ ] Verify deployment URL works
- [ ] Test file upload
- [ ] Check all charts render
- [ ] Verify export works
- [ ] Test on mobile
- [ ] Monitor initial metrics
- [ ] Share with test users

## Next Steps

1. Deploy to Vercel
2. Share URL with chess community
3. Gather user feedback
4. Iterate based on usage
5. Add planned features from roadmap

## Production URL

Once deployed on Vercel, your URL will be:
- Preview: `https://acl-{hash}.vercel.app`
- Production: `https://acl.vercel.app` (or custom domain)

---

**Ready to deploy!** 🚀

The application is production-ready and optimized for Vercel deployment. All code is committed and pushed to the repository.
