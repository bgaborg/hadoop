/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.store.EtagChecksum;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Subclass of S3AFileSystem for diagnosing tests / apps that use after close().
 */
public class S3ACloseEnforcedFileSystem extends S3AFileSystem {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3ACloseEnforcedFileSystem.class);

  /** Initially -1.  initialize() increments, close() decrements. */
  private AtomicInteger closedCount = new AtomicInteger(-1);

  private List<String> closeStackTraces = new ArrayList();

  private void logClosers() {
    int val = closedCount.get();
    LOG.warn("FS was closed {} times by:", val);
    int i = 1;
    for (String s : closeStackTraces) {
      LOG.warn("----------------- close() #{} ---------------", i++);
      LOG.warn(s);
    }

  }

  private void checkIfClosed() {
    int val = closedCount.get();
    if (val > 0) {
      RuntimeException e = new RuntimeException("Using closed FS!.");
      LOG.error("Use after close(): ", e);
      logClosers();
      throw e;
    }
  }

  @Override
  public void close() throws IOException {
    LOG.debug("FS Closed, saving stack trace.");
    closeStackTraces.add(
        ExceptionUtils.getStackTrace(new RuntimeException("close() called" +
            " here")));
    closedCount.incrementAndGet();
    super.close();
  }

  @Override
  public void initialize(URI name, Configuration originalConf)
      throws IOException {
    int val = closedCount.getAndSet(0);
    if (val != -1) {
      LOG.error("initialize() called when closedCount was {}", val);
    }
    LOG.warn("Will log any use-after-close issues.");
    super.initialize(name, originalConf);
  }

  @Override
  protected URI getCanonicalUri() {
    checkIfClosed();
    return super.getCanonicalUri();
  }

  @Override
  public String getName() {
    checkIfClosed();
    return super.getName();
  }

  @Override
  public Path makeQualified(Path path) {
    checkIfClosed();
    return super.makeQualified(path);
  }

  @Override
  public Token<?> getDelegationToken(String renewer) throws IOException {
    checkIfClosed();
    return super.getDelegationToken(renewer);
  }

  @Override
  public Token<?>[] addDelegationTokens(String renewer, Credentials credentials)
      throws IOException {
    checkIfClosed();
    return super.addDelegationTokens(renewer, credentials);
  }

  @Override
  public FileSystem[] getChildFileSystems() {
    checkIfClosed();
    return super.getChildFileSystems();
  }

  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
      long len) throws IOException {
    checkIfClosed();
    return super.getFileBlockLocations(file, start, len);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(Path p, long start, long len)
      throws IOException {
    checkIfClosed();
    return super.getFileBlockLocations(p, start, len);
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    checkIfClosed();
    return super.getServerDefaults();
  }

  @Override
  public FsServerDefaults getServerDefaults(Path p) throws IOException {
    checkIfClosed();
    return super.getServerDefaults(p);
  }

  @Override
  public Path resolvePath(Path p) throws IOException {
    checkIfClosed();
    return super.resolvePath(p);
  }

  @Override
  public FSDataInputStream open(Path f) throws IOException {
    checkIfClosed();
    return super.open(f);
  }

  @Override
  public FSDataInputStream open(PathHandle fd) throws IOException {
    checkIfClosed();
    return super.open(fd);
  }

  @Override
  public FSDataInputStream open(PathHandle fd, int bufferSize)
      throws IOException {
    checkIfClosed();
    return super.open(fd, bufferSize);
  }

  @Override
  protected PathHandle createPathHandle(FileStatus stat,
      Options.HandleOpt... opt) {
    checkIfClosed();
    return super.createPathHandle(stat, opt);
  }

  @Override
  public FSDataOutputStream create(Path f) throws IOException {
    checkIfClosed();
    return super.create(f);
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite)
      throws IOException {
    checkIfClosed();
    return super.create(f, overwrite);
  }

  @Override
  public FSDataOutputStream create(Path f, Progressable progress)
      throws IOException {
    checkIfClosed();
    return super.create(f, progress);
  }

  @Override
  public FSDataOutputStream create(Path f, short replication)
      throws IOException {
    checkIfClosed();
    return super.create(f, replication);
  }

  @Override
  public FSDataOutputStream create(Path f, short replication,
      Progressable progress) throws IOException {
    checkIfClosed();
    return super.create(f, replication, progress);
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize)
      throws IOException {
    checkIfClosed();
    return super.create(f, overwrite, bufferSize);
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize,
      Progressable progress) throws IOException {
    checkIfClosed();
    return super.create(f, overwrite, bufferSize, progress);
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize,
      short replication, long blockSize) throws IOException {
    checkIfClosed();
    return super.create(f, overwrite, bufferSize, replication, blockSize);
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress)
      throws IOException {
    checkIfClosed();
    return super
        .create(f, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      EnumSet<CreateFlag> flags, int bufferSize, short replication,
      long blockSize, Progressable progress) throws IOException {
    checkIfClosed();
    return super
        .create(f, permission, flags, bufferSize, replication, blockSize,
            progress);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      EnumSet<CreateFlag> flags, int bufferSize, short replication,
      long blockSize, Progressable progress, Options.ChecksumOpt checksumOpt)
      throws IOException {
    checkIfClosed();
    return super
        .create(f, permission, flags, bufferSize, replication, blockSize,
            progress, checksumOpt);
  }

  @Override
  protected FSDataOutputStream primitiveCreate(Path f,
      FsPermission absolutePermission, EnumSet<CreateFlag> flag, int bufferSize,
      short replication, long blockSize, Progressable progress,
      Options.ChecksumOpt checksumOpt) throws IOException {
    checkIfClosed();
    return super
        .primitiveCreate(f, absolutePermission, flag, bufferSize, replication,
            blockSize, progress, checksumOpt);
  }

  @Override
  protected boolean primitiveMkdir(Path f, FsPermission absolutePermission)
      throws IOException {
    checkIfClosed();
    return super.primitiveMkdir(f, absolutePermission);
  }

  @Override
  protected void primitiveMkdir(Path f, FsPermission absolutePermission,
      boolean createParent) throws IOException {
    checkIfClosed();
    super.primitiveMkdir(f, absolutePermission, createParent);
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress)
      throws IOException {
    checkIfClosed();
    return super
        .createNonRecursive(f, overwrite, bufferSize, replication, blockSize,
            progress);
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    checkIfClosed();
    return super
        .createNonRecursive(f, permission, overwrite, bufferSize, replication,
            blockSize, progress);
  }

  @Override
  public boolean createNewFile(Path f) throws IOException {
    checkIfClosed();
    return super.createNewFile(f);
  }

  @Override
  public FSDataOutputStream append(Path f) throws IOException {
    checkIfClosed();
    return super.append(f);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize) throws IOException {
    checkIfClosed();
    return super.append(f, bufferSize);
  }

  @Override
  public void concat(Path trg, Path[] psrcs) throws IOException {
    checkIfClosed();
    super.concat(trg, psrcs);
  }

  @Override
  public short getReplication(Path src) throws IOException {
    checkIfClosed();
    return super.getReplication(src);
  }

  @Override
  public boolean setReplication(Path src, short replication)
      throws IOException {
    checkIfClosed();
    return super.setReplication(src, replication);
  }

  @Override
  protected void rename(Path src, Path dst, Options.Rename... options)
      throws IOException {
    checkIfClosed();
    super.rename(src, dst, options);
  }

  @Override
  public boolean truncate(Path f, long newLength) throws IOException {
    checkIfClosed();
    return super.truncate(f, newLength);
  }

  @Override
  public boolean delete(Path f) throws IOException {
    checkIfClosed();
    return super.delete(f);
  }

  @Override
  public boolean deleteOnExit(Path f) throws IOException {
    checkIfClosed();
    return super.deleteOnExit(f);
  }

  @Override
  public boolean cancelDeleteOnExit(Path f) {
    checkIfClosed();
    return super.cancelDeleteOnExit(f);
  }

  @Override
  protected void processDeleteOnExit() {
    checkIfClosed();
    super.processDeleteOnExit();
  }

  @Override
  public long getLength(Path f) throws IOException {
    checkIfClosed();
    return super.getLength(f);
  }

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    checkIfClosed();
    return super.getContentSummary(f);
  }

  @Override
  public QuotaUsage getQuotaUsage(Path f) throws IOException {
    checkIfClosed();
    return super.getQuotaUsage(f);
  }

  @Override
  protected DirectoryEntries listStatusBatch(Path f, byte[] token)
      throws FileNotFoundException, IOException {
    checkIfClosed();
    return super.listStatusBatch(f, token);
  }

  @Override
  public RemoteIterator<Path> listCorruptFileBlocks(Path path)
      throws IOException {
    checkIfClosed();
    return super.listCorruptFileBlocks(path);
  }

  @Override
  public FileStatus[] listStatus(Path f, PathFilter filter)
      throws FileNotFoundException, IOException {
    checkIfClosed();
    return super.listStatus(f, filter);
  }

  @Override
  public FileStatus[] listStatus(Path[] files)
      throws FileNotFoundException, IOException {
    checkIfClosed();
    return super.listStatus(files);
  }

  @Override
  public FileStatus[] listStatus(Path[] files, PathFilter filter)
      throws FileNotFoundException, IOException {
    checkIfClosed();
    return super.listStatus(files, filter);
  }

  @Override
  public RemoteIterator<FileStatus> listStatusIterator(Path p)
      throws FileNotFoundException, IOException {
    checkIfClosed();
    return super.listStatusIterator(p);
  }

  @Override
  public Path getHomeDirectory() {
    checkIfClosed();
    return super.getHomeDirectory();
  }

  @Override
  protected Path getInitialWorkingDirectory() {
    checkIfClosed();
    return super.getInitialWorkingDirectory();
  }

  @Override
  public boolean mkdirs(Path f) throws IOException {
    checkIfClosed();
    return super.mkdirs(f);
  }

  @Override
  public void copyFromLocalFile(Path src, Path dst) throws IOException {
    checkIfClosed();
    super.copyFromLocalFile(src, dst);
  }

  @Override
  public void moveFromLocalFile(Path[] srcs, Path dst) throws IOException {
    checkIfClosed();
    super.moveFromLocalFile(srcs, dst);
  }

  @Override
  public void moveFromLocalFile(Path src, Path dst) throws IOException {
    checkIfClosed();
    super.moveFromLocalFile(src, dst);
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
      throws IOException {
    checkIfClosed();
    super.copyFromLocalFile(delSrc, src, dst);
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs,
      Path dst) throws IOException {
    checkIfClosed();
    super.copyFromLocalFile(delSrc, overwrite, srcs, dst);
  }

  @Override
  public void copyToLocalFile(Path src, Path dst) throws IOException {
    checkIfClosed();
    super.copyToLocalFile(src, dst);
  }

  @Override
  public void moveToLocalFile(Path src, Path dst) throws IOException {
    checkIfClosed();
    super.moveToLocalFile(src, dst);
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst)
      throws IOException {
    checkIfClosed();
    super.copyToLocalFile(delSrc, src, dst);
  }

  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst,
      boolean useRawLocalFileSystem) throws IOException {
    checkIfClosed();
    super.copyToLocalFile(delSrc, src, dst, useRawLocalFileSystem);
  }

  @Override
  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
      throws IOException {
    checkIfClosed();
    return super.startLocalOutput(fsOutputFile, tmpLocalFile);
  }

  @Override
  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
      throws IOException {
    checkIfClosed();
    super.completeLocalOutput(fsOutputFile, tmpLocalFile);
  }

  @Override
  public long getUsed() throws IOException {
    checkIfClosed();
    return super.getUsed();
  }

  @Override
  public long getUsed(Path path) throws IOException {
    checkIfClosed();
    return super.getUsed(path);
  }

  @Override
  public long getBlockSize(Path f) throws IOException {
    checkIfClosed();
    return super.getBlockSize(f);
  }

  @Override
  public long getDefaultBlockSize(Path f) {
    checkIfClosed();
    return super.getDefaultBlockSize(f);
  }

  @Override
  public short getDefaultReplication() {
    checkIfClosed();
    return super.getDefaultReplication();
  }

  @Override
  public short getDefaultReplication(Path path) {
    checkIfClosed();
    return super.getDefaultReplication(path);
  }

  @Override
  public void access(Path path, FsAction mode)
      throws AccessControlException, FileNotFoundException, IOException {
    checkIfClosed();
    super.access(path, mode);
  }

  @Override
  protected Path fixRelativePart(Path p) {
    checkIfClosed();
    return super.fixRelativePart(p);
  }

  @Override
  public void createSymlink(Path target, Path link, boolean createParent)
      throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, ParentNotDirectoryException,
      UnsupportedFileSystemException, IOException {
    checkIfClosed();
    super.createSymlink(target, link, createParent);
  }

  @Override
  public FileStatus getFileLinkStatus(Path f)
      throws AccessControlException, FileNotFoundException,
      UnsupportedFileSystemException, IOException {
    checkIfClosed();
    return super.getFileLinkStatus(f);
  }

  @Override
  public boolean supportsSymlinks() {
    checkIfClosed();
    return super.supportsSymlinks();
  }

  @Override
  public Path getLinkTarget(Path f) throws IOException {
    checkIfClosed();
    return super.getLinkTarget(f);
  }

  @Override
  protected Path resolveLink(Path f) throws IOException {
    checkIfClosed();
    return super.resolveLink(f);
  }

  @Override
  public FileChecksum getFileChecksum(Path f) throws IOException {
    checkIfClosed();
    return super.getFileChecksum(f);
  }

  @Override
  public EtagChecksum getFileChecksum(Path f, long length) throws IOException {
    checkIfClosed();
    return super.getFileChecksum(f, length);
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) {
    checkIfClosed();
    super.setVerifyChecksum(verifyChecksum);
  }

  @Override
  public void setWriteChecksum(boolean writeChecksum) {
    checkIfClosed();
    super.setWriteChecksum(writeChecksum);
  }

  @Override
  public FsStatus getStatus() throws IOException {
    checkIfClosed();
    return super.getStatus();
  }

  @Override
  public FsStatus getStatus(Path p) throws IOException {
    checkIfClosed();
    return super.getStatus(p);
  }

  @Override
  public void setPermission(Path p, FsPermission permission)
      throws IOException {
    checkIfClosed();
    super.setPermission(p, permission);
  }

  @Override
  public void setOwner(Path p, String username, String groupname)
      throws IOException {
    checkIfClosed();
    super.setOwner(p, username, groupname);
  }

  @Override
  public void setTimes(Path p, long mtime, long atime) throws IOException {
    checkIfClosed();
    super.setTimes(p, mtime, atime);
  }

  @Override
  public Path createSnapshot(Path path, String snapshotName)
      throws IOException {
    checkIfClosed();
    return super.createSnapshot(path, snapshotName);
  }

  @Override
  public void renameSnapshot(Path path, String snapshotOldName,
      String snapshotNewName) throws IOException {
    checkIfClosed();
    super.renameSnapshot(path, snapshotOldName, snapshotNewName);
  }

  @Override
  public void deleteSnapshot(Path path, String snapshotName)
      throws IOException {
    checkIfClosed();
    super.deleteSnapshot(path, snapshotName);
  }

  @Override
  public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    checkIfClosed();
    super.modifyAclEntries(path, aclSpec);
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec)
      throws IOException {
    checkIfClosed();
    super.removeAclEntries(path, aclSpec);
  }

  @Override
  public void removeDefaultAcl(Path path) throws IOException {
    checkIfClosed();
    super.removeDefaultAcl(path);
  }

  @Override
  public void removeAcl(Path path) throws IOException {
    checkIfClosed();
    super.removeAcl(path);
  }

  @Override
  public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
    checkIfClosed();
    super.setAcl(path, aclSpec);
  }

  @Override
  public AclStatus getAclStatus(Path path) throws IOException {
    checkIfClosed();
    return super.getAclStatus(path);
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value)
      throws IOException {
    checkIfClosed();
    super.setXAttr(path, name, value);
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value,
      EnumSet<XAttrSetFlag> flag) throws IOException {
    checkIfClosed();
    super.setXAttr(path, name, value, flag);
  }

  @Override
  public byte[] getXAttr(Path path, String name) throws IOException {
    checkIfClosed();
    return super.getXAttr(path, name);
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    checkIfClosed();
    return super.getXAttrs(path);
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path, List<String> names)
      throws IOException {
    checkIfClosed();
    return super.getXAttrs(path, names);
  }

  @Override
  public List<String> listXAttrs(Path path) throws IOException {
    checkIfClosed();
    return super.listXAttrs(path);
  }

  @Override
  public void removeXAttr(Path path, String name) throws IOException {
    checkIfClosed();
    super.removeXAttr(path, name);
  }

  @Override
  public void setStoragePolicy(Path src, String policyName) throws IOException {
    checkIfClosed();
    super.setStoragePolicy(src, policyName);
  }

  @Override
  public void unsetStoragePolicy(Path src) throws IOException {
    checkIfClosed();
    super.unsetStoragePolicy(src);
  }

  @Override
  public BlockStoragePolicySpi getStoragePolicy(Path src) throws IOException {
    checkIfClosed();
    return super.getStoragePolicy(src);
  }

  @Override
  public Collection<? extends BlockStoragePolicySpi> getAllStoragePolicies()
      throws IOException {
    checkIfClosed();
    return super.getAllStoragePolicies();
  }

  @Override
  public Path getTrashRoot(Path path) {
    checkIfClosed();
    return super.getTrashRoot(path);
  }

  @Override
  public Collection<FileStatus> getTrashRoots(boolean allUsers) {
    checkIfClosed();
    return super.getTrashRoots(allUsers);
  }

  @Override
  public FSDataOutputStreamBuilder createFile(Path path) {
    checkIfClosed();
    return super.createFile(path);
  }

  @Override
  public FSDataOutputStreamBuilder appendFile(Path path) {
    checkIfClosed();
    return super.appendFile(path);
  }
}
